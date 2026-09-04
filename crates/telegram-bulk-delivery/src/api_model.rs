use serde_json::{Map, Value};
use std::io::Read;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EnvelopeError {
    #[error("invalid JSON envelope: {0}")]
    Json(#[from] serde_json::Error),
    #[error("duplicate envelope key: {0}")]
    Duplicate(&'static str),
    #[error("parameters must be an object")]
    Parameters,
    #[error("recipients must be a non-empty array")]
    Recipients,
    #[error("recipient patch must be an object")]
    Patch,
    #[error("payload exceeds configured limit")]
    TooLarge,
}

pub fn shallow_merge(
    parameters: &Map<String, Value>,
    patch: &Map<String, Value>,
) -> Map<String, Value> {
    let mut resolved = parameters.clone();
    resolved.extend(patch.clone());
    resolved
}

pub fn parse_envelope<R: Read>(
    reader: R,
    shared_limit: usize,
    patch_limit: usize,
    max_recipients: usize,
    mut recipient: impl FnMut(u32, Map<String, Value>) -> Result<(), EnvelopeError>,
) -> Result<Map<String, Value>, EnvelopeError> {
    use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, Visitor};
    // Custom deserialization above cannot seed an array through MapAccess directly without a seed wrapper.
    struct Wrap<'a, F>(&'a mut F, usize, usize, &'a mut usize);
    impl<'de, F> DeserializeSeed<'de> for Wrap<'_, F>
    where
        F: FnMut(u32, Map<String, Value>) -> Result<(), EnvelopeError>,
    {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
            use serde::de::{Error as _, SeqAccess, Visitor};
            struct W<'a, F>(&'a mut F, usize, usize, &'a mut usize);
            impl<'de, F> Visitor<'de> for W<'_, F>
            where
                F: FnMut(u32, Map<String, Value>) -> Result<(), EnvelopeError>,
            {
                type Value = ();
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("recipient array")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut s: A) -> Result<(), A::Error> {
                    while let Some(v) = s.next_element::<Value>()? {
                        if *self.3 >= self.2 {
                            return Err(A::Error::custom(EnvelopeError::TooLarge));
                        }
                        let p = v
                            .as_object()
                            .ok_or_else(|| A::Error::custom(EnvelopeError::Patch))?
                            .clone();
                        if serde_json::to_vec(&p).map_err(A::Error::custom)?.len() > self.1 {
                            return Err(A::Error::custom(EnvelopeError::TooLarge));
                        }
                        (self.0)(*self.3 as u32, p).map_err(A::Error::custom)?;
                        *self.3 += 1;
                    }
                    Ok(())
                }
            }
            d.deserialize_seq(W(self.0, self.1, self.2, self.3))
        }
    }
    struct Env<'a, F> {
        sl: usize,
        pl: usize,
        max: usize,
        emit: &'a mut F,
    }
    impl<'de, F> DeserializeSeed<'de> for Env<'_, F>
    where
        F: FnMut(u32, Map<String, Value>) -> Result<(), EnvelopeError>,
    {
        type Value = Map<String, Value>;
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            struct E<'a, F>(Env<'a, F>);
            impl<'de, F> Visitor<'de> for E<'_, F>
            where
                F: FnMut(u32, Map<String, Value>) -> Result<(), EnvelopeError>,
            {
                type Value = Map<String, Value>;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("bulk envelope object")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<Self::Value, A::Error> {
                    let (mut p, mut ps, mut rs, mut n) = (Map::new(), false, false, 0usize);
                    while let Some(k) = m.next_key::<String>()? {
                        match k.as_str() {
                            "parameters" => {
                                if ps {
                                    return Err(A::Error::custom(EnvelopeError::Duplicate(
                                        "parameters",
                                    )));
                                }
                                ps = true;
                                let v = m.next_value::<Value>()?;
                                p = v
                                    .as_object()
                                    .ok_or_else(|| A::Error::custom(EnvelopeError::Parameters))?
                                    .clone();
                                if serde_json::to_vec(&p).map_err(A::Error::custom)?.len()
                                    > self.0.sl
                                {
                                    return Err(A::Error::custom(EnvelopeError::TooLarge));
                                }
                            }
                            "recipients" => {
                                if rs {
                                    return Err(A::Error::custom(EnvelopeError::Duplicate(
                                        "recipients",
                                    )));
                                }
                                rs = true;
                                m.next_value_seed(Wrap(self.0.emit, self.0.pl, self.0.max, &mut n))?
                            }
                            _ => {
                                m.next_value::<IgnoredAny>()?;
                            }
                        }
                    }
                    if !rs || n == 0 {
                        return Err(A::Error::custom(EnvelopeError::Recipients));
                    }
                    Ok(p)
                }
            }
            d.deserialize_map(E(self))
        }
    }
    let mut de = serde_json::Deserializer::from_reader(reader);
    let result = Env {
        sl: shared_limit,
        pl: patch_limit,
        max: max_recipients,
        emit: &mut recipient,
    }
    .deserialize(&mut de)?;
    de.end()?;
    Ok(result)
}
