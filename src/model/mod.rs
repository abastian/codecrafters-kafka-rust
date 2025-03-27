pub struct Topic {
    id: uuid::Uuid,
    name: String,
    partitions: Vec<Partition>,
}
impl Topic {
    pub fn new(id: uuid::Uuid, name: String) -> Self {
        Topic {
            id,
            name,
            partitions: Vec::new(),
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    pub fn add_partition(&mut self, partition: Partition) {
        self.partitions.push(partition);
    }
}

pub struct Partition {
    id: i32,
    leader: i32,
    leader_epoch: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
}
impl Partition {
    pub fn new(
        id: i32,
        leader: i32,
        leader_epoch: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        eligible_leader_replicas: Option<Vec<i32>>,
        last_known_elr: Option<Vec<i32>>,
    ) -> Self {
        Partition {
            id,
            leader,
            leader_epoch,
            replicas,
            isr,
            eligible_leader_replicas,
            last_known_elr,
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn leader(&self) -> i32 {
        self.leader
    }

    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    pub fn isr(&self) -> &[i32] {
        &self.isr
    }

    pub fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    pub fn last_known_elr(&self) -> Option<&[i32]> {
        self.last_known_elr.as_deref()
    }
}
