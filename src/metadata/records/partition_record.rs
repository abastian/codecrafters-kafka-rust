use bytes::{Buf, BufMut, BytesMut};
use uuid::Uuid;

use crate::protocol::{
    self,
    r#type::{CompactArray, TaggedField, TaggedFields},
    Readable, ReadableVersion, Writable,
};

pub(crate) const API_KEY: i16 = 3;

#[derive(Debug, Clone)]
pub struct PartitionRecord {
    version: i16,
    partition_id: i32,
    topic_id: Uuid,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    removing_replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    leader: i32,
    leader_epoch: i32,
    partition_epoch: i32,
    directories: Vec<Uuid>,
    leader_recovery_state: u8,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
}
impl PartitionRecord {
    pub fn v0(
        partition_id: i32,
        topic_id: Uuid,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        removing_replicas: Vec<i32>,
        adding_replicas: Vec<i32>,
        leader: i32,
        leader_epoch: i32,
        partition_epoch: i32,
        leader_recovery_state: u8,
    ) -> Self {
        Self {
            version: 0,
            partition_id,
            topic_id,
            replicas,
            isr,
            removing_replicas,
            adding_replicas,
            leader,
            leader_epoch,
            partition_epoch,
            directories: vec![],
            leader_recovery_state,
            eligible_leader_replicas: None,
            last_known_elr: None,
        }
    }

    pub fn v1(
        partition_id: i32,
        topic_id: Uuid,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        removing_replicas: Vec<i32>,
        adding_replicas: Vec<i32>,
        leader: i32,
        leader_epoch: i32,
        partition_epoch: i32,
        directories: Vec<Uuid>,
        leader_recovery_state: u8,
    ) -> Self {
        Self {
            version: 1,
            partition_id,
            topic_id,
            replicas,
            isr,
            removing_replicas,
            adding_replicas,
            leader,
            leader_epoch,
            partition_epoch,
            directories,
            leader_recovery_state,
            eligible_leader_replicas: None,
            last_known_elr: None,
        }
    }

    pub fn v2(
        partition_id: i32,
        topic_id: Uuid,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        removing_replicas: Vec<i32>,
        adding_replicas: Vec<i32>,
        leader: i32,
        leader_epoch: i32,
        partition_epoch: i32,
        directories: Vec<Uuid>,
        leader_recovery_state: u8,
        eligible_leader_replicas: Option<Vec<i32>>,
        last_known_elr: Option<Vec<i32>>,
    ) -> Self {
        Self {
            version: 2,
            partition_id,
            topic_id,
            replicas,
            isr,
            removing_replicas,
            adding_replicas,
            leader,
            leader_epoch,
            partition_epoch,
            directories,
            leader_recovery_state,
            eligible_leader_replicas,
            last_known_elr,
        }
    }

    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    pub fn replicas(&self) -> &[i32] {
        self.replicas.as_ref()
    }

    pub fn isr(&self) -> &[i32] {
        self.isr.as_ref()
    }

    pub fn removing_replicas(&self) -> &[i32] {
        self.removing_replicas.as_ref()
    }

    pub fn adding_replicas(&self) -> &[i32] {
        self.adding_replicas.as_ref()
    }

    pub fn leader(&self) -> i32 {
        self.leader
    }

    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    pub fn partition_epoch(&self) -> i32 {
        self.partition_epoch
    }

    pub fn directories(&self) -> &[Uuid] {
        self.directories.as_ref()
    }

    pub fn leader_recovery_state(&self) -> u8 {
        self.leader_recovery_state
    }

    pub fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    pub fn last_known_elr(&self) -> Option<&[i32]> {
        self.last_known_elr.as_deref()
    }
}
impl ReadableVersion for PartitionRecord {
    fn read_version(buffer: &mut impl Buf, version: i16) -> Result<Self, protocol::Error> {
        if (0..=2).contains(&version) {
            let partition_id = buffer.get_i32();
            let topic_id = Uuid::read(buffer);
            let replicas = CompactArray::<i32>::read_inner(buffer)?.ok_or(
                protocol::Error::IllegalArgument(
                    "non-nullable field replicas was serialized as null",
                ),
            )?;
            let isr = CompactArray::<i32>::read_inner(buffer)?.ok_or(
                protocol::Error::IllegalArgument("non-nullable field isr was serialized as null"),
            )?;
            let removing_replicas = CompactArray::<i32>::read_inner(buffer)?.ok_or(
                protocol::Error::IllegalArgument(
                    "non-nullable field removing_replicas was serialized as null",
                ),
            )?;
            let adding_replicas = CompactArray::<i32>::read_inner(buffer)?.ok_or(
                protocol::Error::IllegalArgument(
                    "non-nullable field adding_replicas was serialized as null",
                ),
            )?;
            let leader = buffer.get_i32();
            let leader_epoch = buffer.get_i32();
            let partition_epoch = buffer.get_i32();
            let directories = if version >= 1 {
                CompactArray::<Uuid>::read_inner(buffer)?.ok_or(
                    protocol::Error::IllegalArgument(
                        "non-nullable field directories was serialized as null",
                    ),
                )?
            } else {
                vec![]
            };

            let mut leader_recovery_state = 0;
            let mut eligible_leader_replicas = None;
            let mut last_known_elr = None;
            let tagged_fields = TaggedFields::read_result_inner(buffer)?;
            for tf in tagged_fields {
                let mut inner_buffer = tf.data;
                match tf.key {
                    0 => {
                        leader_recovery_state = u8::read(&mut inner_buffer);
                    }
                    1 => {
                        if version >= 2 {
                            if let Some(value) = CompactArray::<i32>::read_inner(&mut inner_buffer)?
                            {
                                eligible_leader_replicas.replace(value);
                            }
                        }
                    }
                    2 => {
                        if version >= 2 {
                            if let Some(value) = CompactArray::<i32>::read_inner(&mut inner_buffer)?
                            {
                                last_known_elr.replace(value);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }

            Ok(Self {
                version,
                partition_id,
                topic_id,
                replicas,
                isr,
                removing_replicas,
                adding_replicas,
                leader,
                leader_epoch,
                partition_epoch,
                directories,
                leader_recovery_state,
                eligible_leader_replicas,
                last_known_elr,
            })
        } else {
            Err(protocol::Error::UnsupportedVersion)
        }
    }
}
impl Writable for PartitionRecord {
    fn write(&self, buffer: &mut impl BufMut) {
        self.partition_id.write(buffer);
        self.topic_id.write(buffer);
        CompactArray::write_inner(buffer, Some(self.replicas()));
        CompactArray::write_inner(buffer, Some(self.isr()));
        CompactArray::write_inner(buffer, Some(self.removing_replicas()));
        CompactArray::write_inner(buffer, Some(self.adding_replicas()));
        self.leader.write(buffer);
        self.leader_epoch.write(buffer);
        self.partition_epoch.write(buffer);
        if self.version >= 1 {
            CompactArray::write_inner(buffer, Some(self.directories()));
        }
        let mut tagged_fields = Vec::with_capacity(3);

        let mut data = BytesMut::with_capacity(1);
        self.leader_recovery_state.write(&mut data);
        tagged_fields.push(TaggedField::new(0, data.freeze()));

        if self.version >= 2 {
            let mut data = BytesMut::with_capacity(8);
            CompactArray::write_inner(&mut data, self.eligible_leader_replicas());
            tagged_fields.push(TaggedField::new(1, data.freeze()));

            let mut data = BytesMut::with_capacity(8);
            CompactArray::write_inner(&mut data, self.last_known_elr());
            tagged_fields.push(TaggedField::new(2, data.freeze()));
        }

        TaggedFields::write_inner(buffer, &tagged_fields);
    }
}
