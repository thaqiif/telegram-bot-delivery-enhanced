use crate::{domain::BotId, scheduler::PER_BOT_CONCURRENCY};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
struct Entry {
    weight: u8,
    deficit: u16,
    inflight: u8,
}
/// Weighted deficit round-robin over bots that are runnable now.
#[derive(Debug, Default)]
pub struct Wdrr {
    ring: VecDeque<BotId>,
    entries: HashMap<BotId, Entry>,
    present: HashSet<BotId>,
    wakes: BinaryHeap<Reverse<(u64, BotId)>>,
    deficit_cap: u16,
}
impl Wdrr {
    pub fn new() -> Self {
        Self {
            deficit_cap: 64,
            ..Self::default()
        }
    }
    pub fn upsert_ready(&mut self, bot: BotId, weight: u8) {
        let existed = self.entries.contains_key(&bot);
        let sleeping = self.is_sleeping(bot);
        let e = self.entries.entry(bot).or_insert(Entry {
            weight: weight.clamp(1, 16),
            deficit: 0,
            inflight: 0,
        });
        e.weight = weight.clamp(1, 16);
        if !sleeping && e.inflight < PER_BOT_CONCURRENCY && self.present.insert(bot) {
            if existed {
                self.ring.push_front(bot)
            } else {
                self.ring.push_back(bot)
            }
        }
    }
    pub fn remove(&mut self, bot: BotId) {
        self.present.remove(&bot);
        self.ring.retain(|b| *b != bot)
    }
    /// Re-admit a bot that is already known to the scheduler (it woke or was
    /// just denied) without touching its stored fairness weight. The deny and
    /// wake paths used to call `upsert_ready(bot, 1)`, silently clobbering a
    /// configured `fairness_weight` until the next durable reconcile.
    pub fn readmit(&mut self, bot: BotId) {
        let Some(e) = self.entries.get(&bot) else {
            return;
        };
        if e.inflight < PER_BOT_CONCURRENCY && !self.is_sleeping(bot) && self.present.insert(bot) {
            self.ring.push_back(bot);
        }
    }
    pub fn sleep_until(&mut self, bot: BotId, at_millis: u64) {
        // Remove from the runnable ring, but preserve the entry/deficit. A
        // durable reconciliation may observe delayed work while this bot is
        // sleeping; `upsert_ready` must not reinsert it before its wake time.
        self.remove(bot);
        self.wakes.push(Reverse((at_millis, bot)))
    }
    pub fn is_sleeping(&self, bot: BotId) -> bool {
        self.wakes.iter().any(|Reverse((_, b))| *b == bot)
    }
    pub fn wake_due(&mut self, now_millis: u64) -> Vec<BotId> {
        let mut due = Vec::new();
        while self
            .wakes
            .peek()
            .is_some_and(|Reverse((at, _))| *at <= now_millis)
        {
            let Reverse((_, b)) = self.wakes.pop().expect("peeked");
            // A bot can have multiple historical wake entries; wake only when
            // no later pause still covers it.
            if !self.is_sleeping(b) && !due.contains(&b) {
                due.push(b)
            }
        }
        due
    }
    pub fn grant(&mut self) -> Option<BotId> {
        let scans = self.ring.len();
        for _ in 0..scans {
            let bot = self.ring.pop_front()?;
            if !self.present.contains(&bot) {
                continue;
            }
            let e = self.entries.get_mut(&bot)?;
            e.deficit = e
                .deficit
                .saturating_add(u16::from(e.weight))
                .min(self.deficit_cap);
            self.ring.push_back(bot);
            if e.inflight < PER_BOT_CONCURRENCY && e.deficit > 0 {
                e.deficit -= 1;
                e.inflight += 1;
                return Some(bot);
            }
        }
        None
    }
    pub fn complete(&mut self, bot: BotId) {
        if let Some(e) = self.entries.get_mut(&bot) {
            e.inflight = e.inflight.saturating_sub(1)
        }
    }
    pub fn inflight(&self, bot: BotId) -> u8 {
        self.entries.get(&bot).map_or(0, |e| e.inflight)
    }
    pub fn contains(&self, bot: BotId) -> bool {
        self.present.contains(&bot)
    }
    pub fn reconcile(&mut self, ready: impl IntoIterator<Item = (BotId, u8)>) {
        for (b, w) in ready {
            self.upsert_ready(b, w)
        }
    }
    /// Number of bots currently runnable (queue-depth gauge for observability).
    pub fn ready_len(&self) -> usize {
        self.present.len()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: u32) -> BotId {
        let mut b = [0; 32];
        b[..4].copy_from_slice(&n.to_be_bytes());
        BotId(b)
    }
    #[test]
    fn thousand_are_fair() {
        let mut q = Wdrr::new();
        for n in 0..1000 {
            q.upsert_ready(id(n), 1)
        }
        let mut counts = vec![0u16; 1000];
        for _ in 0..2000 {
            let b = q.grant().unwrap();
            q.complete(b);
            let n = u32::from_be_bytes(b.0[..4].try_into().unwrap()) as usize;
            counts[n] += 1
        }
        assert!(counts.iter().all(|n| *n >= 1));
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 34)
    }
    #[test]
    fn readmit_preserves_configured_weight() {
        let mut q = Wdrr::new();
        q.upsert_ready(id(1), 5);
        // A wake/deny re-admission must not reset the weight to 1.
        q.readmit(id(1));
        assert_eq!(q.entries[&id(1)].weight, 5);
        q.sleep_until(id(1), 10);
        for b in q.wake_due(10) {
            q.readmit(b);
        }
        assert_eq!(q.entries[&id(1)].weight, 5);
        assert!(q.contains(id(1)));
    }

    #[test]
    fn sleep_reenters_without_duplicates() {
        let mut q = Wdrr::new();
        q.upsert_ready(id(1), 1);
        q.sleep_until(id(1), 10);
        assert!(!q.contains(id(1)));
        assert!(q.wake_due(9).is_empty());
        for b in q.wake_due(10) {
            q.upsert_ready(b, 1)
        }
        assert!(q.contains(id(1)));
    }
}
