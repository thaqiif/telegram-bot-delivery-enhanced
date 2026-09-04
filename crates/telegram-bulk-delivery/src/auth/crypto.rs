use crate::domain::BotId;
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use hkdf::Hkdf;
use rand::{rngs::OsRng, RngCore};
use sha2::Sha256;
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

#[derive(Clone, Copy)]
pub enum Purpose {
    BotToken,
    WebhookSecret,
}
#[derive(Debug, Clone)]
pub struct EncryptedSecret {
    pub key_id: String,
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
}
#[derive(Zeroize, ZeroizeOnDrop)]
struct DerivedKeys {
    bot_id: [u8; 32],
    aead: [u8; 32],
    blob_hmac: [u8; 32],
}

pub struct KeyRing {
    key_id: String,
    keys: DerivedKeys,
}
impl std::fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redacted: `DerivedKeys` are secret-equivalent and must never surface
        // through a stray debug!(?keys), panic context, or log.
        f.debug_struct("KeyRing")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}
impl Drop for KeyRing {
    fn drop(&mut self) {
        self.key_id.zeroize();
    }
}

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("key derivation failed")]
    Derivation,
    #[error("authenticated encryption failed")]
    Encrypt,
    #[error("authenticated decryption failed")]
    Decrypt,
    #[error("encrypted value uses unknown key id")]
    UnknownKey,
}

impl KeyRing {
    pub fn derive(master: &[u8; 32], key_id: impl Into<String>) -> Result<Self, CryptoError> {
        let hkdf = Hkdf::<Sha256>::new(Some(b"telegram-bulk-delivery/v1"), master);
        let mut keys = DerivedKeys {
            bot_id: [0; 32],
            aead: [0; 32],
            blob_hmac: [0; 32],
        };
        hkdf.expand(b"bot-id-key", &mut keys.bot_id)
            .map_err(|_| CryptoError::Derivation)?;
        hkdf.expand(b"aead-key", &mut keys.aead)
            .map_err(|_| CryptoError::Derivation)?;
        hkdf.expand(b"blob-hmac-key", &mut keys.blob_hmac)
            .map_err(|_| CryptoError::Derivation)?;
        Ok(Self {
            key_id: key_id.into(),
            keys,
        })
    }

    pub fn bot_id(&self, token: &[u8]) -> BotId {
        BotId(*blake3::keyed_hash(&self.keys.bot_id, token).as_bytes())
    }

    pub fn encrypt(
        &self,
        bot_id: BotId,
        purpose: Purpose,
        plaintext: &[u8],
    ) -> Result<EncryptedSecret, CryptoError> {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let aad = aad(bot_id, purpose, &self.key_id);
        let ciphertext = Aes256Gcm::new_from_slice(&self.keys.aead)
            .map_err(|_| CryptoError::Encrypt)?
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::Encrypt)?;
        Ok(EncryptedSecret {
            key_id: self.key_id.clone(),
            nonce,
            ciphertext,
        })
    }

    pub fn decrypt(
        &self,
        bot_id: BotId,
        purpose: Purpose,
        value: &EncryptedSecret,
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        if value.key_id != self.key_id {
            return Err(CryptoError::UnknownKey);
        }
        let aad = aad(bot_id, purpose, &value.key_id);
        let plaintext = Aes256Gcm::new_from_slice(&self.keys.aead)
            .map_err(|_| CryptoError::Decrypt)?
            .decrypt(
                Nonce::from_slice(&value.nonce),
                Payload {
                    msg: &value.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::Decrypt)?;
        Ok(Zeroizing::new(plaintext))
    }
}
fn aad(bot_id: BotId, purpose: Purpose, key_id: &str) -> Zeroizing<Vec<u8>> {
    let label: &[u8] = match purpose {
        Purpose::BotToken => b"token",
        Purpose::WebhookSecret => b"webhook",
    };
    let mut aad = Zeroizing::new(Vec::with_capacity(32 + label.len() + key_id.len()));
    aad.extend_from_slice(&bot_id.0);
    aad.extend_from_slice(label);
    aad.extend_from_slice(key_id.as_bytes());
    aad
}
