use crate::domain::BotId;
use std::collections::HashMap;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Global,
    Bot(BotId),
    Chat(BotId, String),
    Group(BotId, String),
    Method(BotId, String),
}
impl std::str::FromStr for Scope {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "global" {
            return Ok(Scope::Global);
        }
        if let Some(rest) = s.strip_prefix("bot:") {
            return Ok(Scope::Bot(BotId(
                hex::decode(rest)
                    .map_err(|e| e.to_string())?
                    .try_into()
                    .map_err(|_| "bad bot id")?,
            )));
        }
        if let Some(rest) = s.strip_prefix("chat:") {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() == 2 {
                return Ok(Scope::Chat(
                    BotId(
                        hex::decode(parts[0])
                            .map_err(|e| e.to_string())?
                            .try_into()
                            .map_err(|_| "bad bot id")?,
                    ),
                    parts[1].into(),
                ));
            }
        }
        if let Some(rest) = s.strip_prefix("group:") {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() == 2 {
                return Ok(Scope::Group(
                    BotId(
                        hex::decode(parts[0])
                            .map_err(|e| e.to_string())?
                            .try_into()
                            .map_err(|_| "bad bot id")?,
                    ),
                    parts[1].into(),
                ));
            }
        }
        if let Some(rest) = s.strip_prefix("method:") {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() == 2 {
                return Ok(Scope::Method(
                    BotId(
                        hex::decode(parts[0])
                            .map_err(|e| e.to_string())?
                            .try_into()
                            .map_err(|_| "bad bot id")?,
                    ),
                    parts[1].into(),
                ));
            }
        }
        Err("invalid scope".into())
    }
}
#[derive(Debug, Clone, Copy)]
struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: u64,
    paused_until: u64,
    inflight: u8,
    max_inflight: u8,
}
impl Bucket {
    fn new(rate: f64, burst: f64, now: u64, max: u8) -> Self {
        Self {
            rate,
            burst,
            tokens: burst,
            last: now,
            paused_until: 0,
            inflight: 0,
            max_inflight: max,
        }
    }
    fn check(&mut self, now: u64) -> Result<(), u64> {
        if now < self.paused_until {
            return Err(self.paused_until);
        };
        let elapsed = now.saturating_sub(self.last) as f64 / 1000.;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last = now;
        if self.inflight >= self.max_inflight {
            return Err(now + 1);
        }
        if self.tokens < 1. {
            return Err(now + ((1. - self.tokens) / self.rate * 1000.).ceil() as u64);
        }
        Ok(())
    }
    fn take(&mut self) {
        self.tokens -= 1.;
        self.inflight += 1
    }
}
#[derive(Debug)]
pub struct Limiters {
    global: Bucket,
    bots: HashMap<BotId, Bucket>,
    chats: HashMap<(BotId, String), Bucket>,
    groups: HashMap<(BotId, String), Bucket>,
    methods: HashMap<(BotId, String), Bucket>,
    chat_capacity: usize,
}
impl Limiters {
    pub const IDLE_BUCKET_TTL_MS: u64 = 60 * 60 * 1000;

    pub fn new(now: u64) -> Self {
        Self {
            global: Bucket::new(25., 50., now, 32),
            bots: HashMap::new(),
            chats: HashMap::with_capacity(131072),
            groups: HashMap::new(),
            methods: HashMap::new(),
            chat_capacity: 131072,
        }
    }
    pub fn check_and_acquire(
        &mut self,
        bot: BotId,
        chat: Option<&str>,
        group: bool,
        method: &str,
        media: bool,
        now: u64,
    ) -> Result<Vec<Scope>, (Scope, u64)> {
        let mut scopes = vec![
            Scope::Global,
            Scope::Bot(bot),
            Scope::Method(bot, method.to_ascii_lowercase()),
        ];
        if let Some(c) = chat {
            scopes.push(Scope::Chat(bot, c.into()));
            if group {
                scopes.push(Scope::Group(bot, c.into()))
            }
        }
        for s in &scopes {
            let r = match s {
                Scope::Global => self.global.check(now),
                Scope::Bot(b) => self
                    .bots
                    .entry(*b)
                    .or_insert_with(|| Bucket::new(20., 10., now, 4))
                    .check(now),
                Scope::Chat(b, c) => {
                    if self.chats.len() >= self.chat_capacity
                        && !self.chats.contains_key(&(*b, c.clone()))
                    {
                        return Err((s.clone(), now + 1000));
                    }
                    self.chats
                        .entry((*b, c.clone()))
                        .or_insert_with(|| Bucket::new(1., 2., now, 4))
                        .check(now)
                }
                Scope::Group(b, c) => self
                    .groups
                    .entry((*b, c.clone()))
                    .or_insert_with(|| Bucket::new(1. / 3., 1., now, 4))
                    .check(now),
                Scope::Method(b, m) => self
                    .methods
                    .entry((*b, m.clone()))
                    .or_insert_with(|| Bucket::new(1000., 1000., now, if media { 4 } else { 8 }))
                    .check(now),
            };
            if let Err(at) = r {
                return Err((s.clone(), at));
            }
        }
        for s in &scopes {
            match s {
                Scope::Global => self.global.take(),
                Scope::Bot(b) => self.bots.get_mut(b).unwrap().take(),
                Scope::Chat(b, c) => self.chats.get_mut(&(*b, c.clone())).unwrap().take(),
                Scope::Group(b, c) => self.groups.get_mut(&(*b, c.clone())).unwrap().take(),
                Scope::Method(b, m) => self.methods.get_mut(&(*b, m.clone())).unwrap().take(),
            }
        }
        Ok(scopes)
    }
    pub fn release(&mut self, scopes: &[Scope]) {
        for s in scopes {
            let b = match s {
                Scope::Global => Some(&mut self.global),
                Scope::Bot(k) => self.bots.get_mut(k),
                Scope::Chat(k, c) => self.chats.get_mut(&(*k, c.clone())),
                Scope::Group(k, c) => self.groups.get_mut(&(*k, c.clone())),
                Scope::Method(k, m) => self.methods.get_mut(&(*k, m.clone())),
            };
            if let Some(b) = b {
                b.inflight = b.inflight.saturating_sub(1)
            }
        }
    }
    pub fn pause(&mut self, scope: &Scope, until: u64, now: u64) {
        let b = match scope {
            Scope::Global => &mut self.global,
            Scope::Bot(k) => self
                .bots
                .entry(*k)
                .or_insert_with(|| Bucket::new(20., 10., now, 4)),
            Scope::Chat(k, c) => {
                // At capacity for unknown chats, do NOT insert — the deny in
                // check_and_acquire already throttles this chat; inserting via
                // pause would grow the map past its bound every time.
                if self.chats.len() >= self.chat_capacity
                    && !self.chats.contains_key(&(*k, c.clone()))
                {
                    return;
                }
                self.chats
                    .entry((*k, c.clone()))
                    .or_insert_with(|| Bucket::new(1., 2., now, 4))
            }
            Scope::Group(k, c) => self
                .groups
                .entry((*k, c.clone()))
                .or_insert_with(|| Bucket::new(1. / 3., 1., now, 4)),
            Scope::Method(k, m) => self
                .methods
                .entry((*k, m.clone()))
                .or_insert_with(|| Bucket::new(1000., 1000., now, 8)),
        };
        b.paused_until = b.paused_until.max(until)
    }
    /// Evict idle buckets so limiter memory tracks the recent working set.
    /// Buckets with in-flight sends or active flood pauses are never evicted.
    pub fn evict_idle(&mut self, now: u64, idle_ms: u64) {
        let cutoff = now.saturating_sub(idle_ms);
        let evict = |b: &Bucket| b.inflight == 0 && b.paused_until <= now && b.last < cutoff;
        self.chats.retain(|_, b| !evict(b));
        self.groups.retain(|_, b| !evict(b));
        self.methods.retain(|_, b| !evict(b));
    }
    pub fn chat_entries(&self) -> usize {
        self.chats.len()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn b() -> BotId {
        BotId([1; 32])
    }
    #[test]
    fn same_chat_is_paced() {
        let mut l = Limiters::new(0);
        let s = l
            .check_and_acquire(b(), Some("1"), false, "sendMessage", false, 0)
            .unwrap();
        l.release(&s);
        let s = l
            .check_and_acquire(b(), Some("1"), false, "sendMessage", false, 0)
            .unwrap();
        l.release(&s);
        assert!(l
            .check_and_acquire(b(), Some("1"), false, "sendMessage", false, 0)
            .is_err())
    }
    #[test]
    fn future_pause_is_kept() {
        let mut l = Limiters::new(0);
        let s = Scope::Chat(b(), "x".into());
        l.pause(&s, 5000, 0);
        assert_eq!(
            l.check_and_acquire(b(), Some("x"), false, "sendMessage", false, 1)
                .unwrap_err()
                .1,
            5000
        )
    }
}
