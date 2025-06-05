//! Performance test program inspired by `ddsperf` in CycloneDDS

use std::time::{/*Instant,*/ Duration};

use log::error;
use rustdds::{
  policy::Reliability, policy::History,
  //DataWriterStatus, 
  DataReaderStatus,
  with_key::Sample,
  DomainParticipantBuilder, Keyed, QosPolicyBuilder,
  //StatusEvented, 
  TopicKind,
};

use clap::{Parser, Subcommand};

use serde::{Deserialize, Serialize};
use smol::Timer;
use futures::{/*FutureExt,*/ StreamExt, TryFutureExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct KeyedSeq {
  pub seq: u32,
  pub keyval: u32,
  pub baggage: Vec<u8>,
}

impl Keyed for KeyedSeq {
  type K = u32;
  fn key(&self) -> Self::K {
    self.keyval
  }
}

// --------------------------------------------------
// --------------------------------------------------

// command-line options
#[derive(Parser)]
struct CommandLineArgs {
    #[command(subcommand)]
    main_mode: MainMode,
}

#[derive(Subcommand, Clone, Debug)]
enum MainMode {
  Pub {
    rate: u32,
    #[command(subcommand)]
    pub_mode_args: Option<PubModeArgs>,
  },
  Sub {},
}

#[derive(Subcommand, Clone, Debug)]
enum PubModeArgs {
  Size {size: u32},
}



fn main() {
  let command_line_args = CommandLineArgs::parse();

  let domain_participant = DomainParticipantBuilder::new(0)
    .build()
    .unwrap_or_else(|e| panic!("DomainParticipant construction failed: {e:?}"));

  let qos = QosPolicyBuilder::new()
    .history(History::KeepLast {depth: 2})
    .reliability(Reliability::Reliable {
      max_blocking_time: rustdds::Duration::from_secs(1),
    })
    .build();

  let topic = domain_participant
    .create_topic(
      // We can internally call the Rust type "HelloWorldData" whatever we want,
      // but these strings must match whatever our counterparts expect
      // to see over RTPS.
      "DDSPerfRDataKS".to_string(),  // topic name
      "KeyedSeq".to_string(), // type name
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));


  match command_line_args.main_mode {
    MainMode::Sub {} => {
      let subscriber = domain_participant.create_subscriber(&qos).unwrap();
      let data_reader = subscriber
        .create_datareader_cdr::<KeyedSeq>(&topic, None) // None = get qos policy from publisher
        .unwrap();

      smol::block_on(async {
        let mut sample_stream = data_reader.async_sample_stream();
        let mut event_stream = sample_stream.async_event_stream();
        let mut ticker = 
            StreamExt::fuse(async_io::Timer::interval(Duration::from_secs(1)));
            

        let mut sample_count = 0_u64;
        let mut byte_count = 0_u64;

        println!("Waiting for messages.");
        loop {
          futures::select! {
            // _ = stop_receiver.recv().fuse() =>
            //   break,

            _tick = ticker.select_next_some() => {
              println!("{sample_count:9} samples {byte_count:9} bytes");
              sample_count = 0;
              byte_count = 0;
            }

            result = sample_stream.select_next_some() => {
              match result {
                Ok(s) => match s.into_value() {
                  Sample::Value(keyed_seq_msg) => {
                    sample_count += 1;
                    // estimate size of message on the wire:
                    // 8 bytes for u32 + u32
                    // 4 bytes for baggage sequence size
                    byte_count += (8 + 4 + keyed_seq_msg.baggage.len()) as u64;
                  }
                  Sample::Dispose(key) =>
                    println!("Disposed with key={key}"),
                }
                Err(e) =>
                  println!("Oh no, DDS read error: {e:?}"),
              }
            }

            e = event_stream.select_next_some() => {
              match e {
                DataReaderStatus::SubscriptionMatched{ writer, current,..} => {
                  if current.count_change() > 0 {
                    println!("Matched with publisher {writer:?}");
                  } else {
                    println!("Lost publisher {writer:?}");
                  }
                }
                _ =>
                  println!("DataReader event: {e:?}"),
              }
            }
          } // select!
        } // loop

      });
    }

    MainMode::Pub{rate, pub_mode_args} => {
      let publisher = domain_participant.create_publisher(&qos).unwrap();
      let writer = publisher
        .create_datawriter_cdr::<KeyedSeq>(&topic, None) // None = get qos policy from publisher
        .unwrap();

      let baggage_size : usize = match pub_mode_args {
        None => 0,
        Some(PubModeArgs::Size{size}) => size as usize,
      };

      let mut baggage = Vec::with_capacity(baggage_size);
      baggage.resize(baggage_size, b'x');
      println!("baggage size = {} bytes", baggage.len());
      let keyed_seq_msg = KeyedSeq {
        keyval: 1234,
        seq: 0,
        baggage,
      };

      smol::block_on(async {
        //let mut datawriter_event_stream = writer.as_async_status_stream();
        //let (write_trigger_sender, write_trigger_receiver) = smol::channel::bounded(1);
        //let mut match_timeout_timer = futures::FutureExt::fuse(Timer::after(Duration::from_secs(10)));
        let mut seq = 0;
        loop {
          // futures::select! {
          //   // _ = match_timeout_timer => {
          //   //   println!("Timeout waiting for subscriber at appear.");
          //   //   break
          //   // }
          //   _ = write_trigger_receiver.recv().fuse() => {
          //     println!("Sending hello");
              let mut new_message = keyed_seq_msg.clone();
              new_message.seq = seq;
              seq += 1;
              writer.async_write(new_message, None)
                .unwrap_or_else(|e| error!("DataWriter async_write failed: {e:?}"))
                .await;
              // wait for 1 sec for transfer to complete before exiting.
              let interval = 1_000_000_000 / rate;
              Timer::after(Duration::from_nanos(interval.into())).await;
            // }

            // e = datawriter_event_stream.select_next_some() => {
            //   match e {
            //     // If we get a matching subscription, trigger the send
            //     DataWriterStatus::PublicationMatched{..} => {
            //       println!("Matched with subscriber");
            //     }
            //     _ =>
            //       println!("DataWriter event: {e:?}"),
            //   }
            // }
          // } // select!
        } // loop

        //println!("Bye, World!");
      });

    } // Pub
  }

}

