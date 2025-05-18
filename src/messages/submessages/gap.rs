use enumflags2::BitFlags;
use log::error;
use speedy::{Readable, Writable};

use crate::{
  messages::submessages::submessages::SubmessageHeader,
  rtps::{Submessage, SubmessageBody},
  structure::{
    guid::EntityId,
    sequence_number::{SequenceNumber, SequenceNumberSet},
  },
};
use super::{
  submessage::{HasEntityIds, WriterSubmessage},
  submessage_flag::GAP_Flags,
  submessage_kind::SubmessageKind,
};
/// This Submessage is sent from an RTPS Writer to an RTPS Reader and
/// indicates to the RTPS Reader that a set of sequence numbers
/// is no longer relevant. The set may contain a contiguous range of sequence
/// numbers and/or a noncontiguous collection of sequence numbers.
#[derive(Debug, PartialEq, Eq, Clone, Readable, Writable)]
pub struct Gap {
  /// Identifies the Reader Entity that is being informed of the
  /// irrelevance of a set of sequence numbers.
  pub reader_id: EntityId,

  /// Identifies the Writer Entity to which the range of sequence
  /// numbers applies.
  pub writer_id: EntityId,

  /// Identifies the first sequence number in the contiguous range of
  /// irrelevant sequence numbers
  pub gap_start: SequenceNumber,

  /// Identifies the last sequence number in the contiguous range of irrelevant
  /// sequence numbers as defined by the spec: gap_list.base is the exclusive
  /// endpoint of this range. Therefore, the range contains the sequence
  /// numbers which satisfy gap_start <= sn < gap_list.base.
  ///
  /// Identifies an additional list of sequence numbers that are
  /// irrelevant. Note that gap_list.base may or may not be included in this
  /// list (and if it is, then the contiguous range of irrelevant
  /// sequence numbers is actually larger than gap_start <= sn < gap_list.base).
  pub gap_list: SequenceNumberSet,
}

impl Gap {
  pub fn create_submessage(self, flags: BitFlags<GAP_Flags>) -> Option<Submessage> {
    let submessage_len = match self.write_to_vec() {
      Ok(bytes) => bytes.len() as u16,
      Err(e) => {
        error!("Reader couldn't write GAP to bytes: {}", e);
        return None;
      }
    };

    Some(Submessage {
      header: SubmessageHeader {
        kind: SubmessageKind::GAP,
        flags: flags.bits(),
        content_length: submessage_len,
      },
      body: SubmessageBody::Writer(WriterSubmessage::Gap(self, flags)),
      original_bytes: None,
    })
  }
}

impl HasEntityIds for Gap {
  fn receiver_entity_id(&self) -> EntityId {
    self.reader_id
  }
  fn sender_entity_id(&self) -> EntityId {
    self.writer_id
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  serialization_test!( type = Gap,
  {
      gap,
      Gap {
          reader_id: EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
          writer_id: EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
          gap_start: SequenceNumber::from(42),
          gap_list: SequenceNumberSet::new_empty(SequenceNumber::from(7))
      },
      le = [0x00, 0x00, 0x03, 0xC7,
            0x00, 0x00, 0x03, 0xC2,
            0x00, 0x00, 0x00, 0x00,
            0x2A, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00],
      be = [0x00, 0x00, 0x03, 0xC7,
            0x00, 0x00, 0x03, 0xC2,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x2A,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00]
  });
}
