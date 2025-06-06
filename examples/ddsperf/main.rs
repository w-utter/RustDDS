//! Performance test program inspired by `ddsperf` in CycloneDDS

use std::time::{/* Instant, */ Duration};

use log::error;
use rustdds::{
  policy::History,
  policy::Reliability,
  with_key::Sample,
  //DataWriterStatus,
  DataReaderStatus,
  DomainParticipantBuilder,
  Keyed,
  QosPolicyBuilder,
  Timestamp,
  //StatusEvented,
  TopicKind,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use smol::Timer;
use futures::{/* FutureExt, */ StreamExt, TryFutureExt};

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
  #[arg(short = 'u', long)]
  best_effort: bool,
  // This flag is called 'u' because it is so in CycloneDDS version also.
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

  Sub,

  Ping {
    rate: u32,
    #[command(subcommand)]
    ping_mode_args: Option<PubModeArgs>,
  },

  Pong,
}

#[derive(Subcommand, Clone, Debug)]
enum PubModeArgs {
  Size { size: u32 },
}

fn main() {
  let command_line_args = CommandLineArgs::parse();

  let domain_participant = DomainParticipantBuilder::new(0)
    .build()
    .unwrap_or_else(|e| panic!("DomainParticipant construction failed: {e:?}"));

  let qos = QosPolicyBuilder::new()
    .history(History::KeepLast { depth: 2 })
    .reliability(if command_line_args.best_effort {
      Reliability::BestEffort
    } else {
      Reliability::Reliable {
        max_blocking_time: rustdds::Duration::from_secs(1),
      }
    })
    .build();

  let reliability_marker = if command_line_args.best_effort {
    'U'
  } else {
    'R'
  };

  let topic_suffix = "KS"; // TODO: Support others also

  let perf_data_topic = domain_participant
    .create_topic(
      format!("DDSPerf{reliability_marker}Data{topic_suffix}"), // topic name
      "KeyedSeq".to_string(),                                   // type name
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));

  let ping_topic = domain_participant
    .create_topic(
      format!("DDSPerf{reliability_marker}Ping{topic_suffix}"), // topic name
      "KeyedSeq".to_string(),                                   // type name
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));
  let pong_topic = domain_participant
    .create_topic(
      format!("DDSPerf{reliability_marker}Pong{topic_suffix}"), // topic name
      "KeyedSeq".to_string(),                                   // type name
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));

  match command_line_args.main_mode {
    MainMode::Sub => {
      let subscriber = domain_participant.create_subscriber(&qos).unwrap();
      let data_reader = subscriber
        .create_datareader_cdr::<KeyedSeq>(&perf_data_topic, None) // None = get qos policy from publisher
        .unwrap();

      smol::block_on(async {
        let mut sample_stream = data_reader.async_sample_stream();
        let mut event_stream = sample_stream.async_event_stream();
        let mut ticker = StreamExt::fuse(async_io::Timer::interval(Duration::from_secs(1)));

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

    MainMode::Pub {
      rate,
      pub_mode_args,
    } => {
      let publisher = domain_participant.create_publisher(&qos).unwrap();
      let writer = publisher
        .create_datawriter_cdr::<KeyedSeq>(&perf_data_topic, None) // None = get qos policy from publisher
        .unwrap();

      let baggage_size: usize = match pub_mode_args {
        None => 0,
        Some(PubModeArgs::Size { size }) => size as usize,
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
        let mut seq = 0;
        loop {
          let mut new_message = keyed_seq_msg.clone();
          new_message.seq = seq;
          seq += 1;
          writer
            .async_write(new_message, None)
            .unwrap_or_else(|e| error!("DataWriter async_write failed: {e:?}"))
            .await;
          // wait for 1 sec for transfer to complete before exiting.
          let interval = 1_000_000_000 / rate;
          Timer::after(Duration::from_nanos(interval.into())).await;
        } // loop
      });
    } // Pub

    MainMode::Ping {
      rate,
      ping_mode_args,
    } => {
      let subscriber = domain_participant.create_subscriber(&qos).unwrap();
      let data_reader = subscriber
        .create_datareader_cdr::<KeyedSeq>(&pong_topic, None) // None = get qos policy from publisher
        .unwrap();
      let publisher = domain_participant.create_publisher(&qos).unwrap();
      let data_writer = publisher
        .create_datawriter_cdr::<KeyedSeq>(&ping_topic, None) // None = get qos policy from publisher
        .unwrap();

      let baggage_size: usize = match ping_mode_args {
        None => 0,
        Some(PubModeArgs::Size { size }) => size as usize,
      };

      smol::block_on(async {
        let mut sample_stream = data_reader.async_sample_stream();
        let mut event_stream = sample_stream.async_event_stream();
        let mut ticker = StreamExt::fuse(async_io::Timer::interval(Duration::from_secs(1)));
        let ping_interval = 1_000_000_000 / rate;
        let mut ping_ticker = StreamExt::fuse(async_io::Timer::interval(Duration::from_nanos(
          ping_interval.into(),
        )));

        let mut ping_seq = 1;
        let mut sample_count = 0_u64;
        let mut byte_count = 0_u64;
        let mut rtt_total = rustdds::Duration::from_secs(0);
        let mut rtt_max = rustdds::Duration::from_secs(0);

        println!("Waiting for messages.");
        loop {
          futures::select! {

            // periodic output
            _tick = ticker.select_next_some() => {
              let rtt_avg =
                if sample_count > 0 {
                  rtt_total.to_std().as_millis() as u64 / sample_count
                } else {
                  0
                };
              println!("{sample_count:9} samples {byte_count:9} bytes   round-trip-time avg {rtt_avg:5} ms, max {:5} ms",
                  rtt_max.to_std().as_millis());
              sample_count = 0;
              byte_count = 0;
              rtt_total = rustdds::Duration::from_secs(0);
              rtt_max = rustdds::Duration::from_secs(0);
            }

            // generate ping
            _tick = ping_ticker.select_next_some() => {
              let mut baggage = Vec::with_capacity(baggage_size);
              baggage.resize(baggage_size, b'x');
              //println!("baggage size = {} bytes", baggage.len());
              let keyed_seq_msg = KeyedSeq {
                keyval: 1234,
                seq: ping_seq,
                baggage,
              };
              ping_seq += 1;
              let ts = Timestamp::now().into();
              data_writer.async_write(keyed_seq_msg, Some(ts))
                .await.unwrap();
            }

            // handle pong
            result = sample_stream.select_next_some() => {
              match result {
                Ok(s) => match s.value() {
                  Sample::Value(keyed_seq_msg) => {
                    sample_count += 1;
                    // estimate size of message on the wire:
                    // 8 bytes for u32 + u32
                    // 4 bytes for baggage sequence size
                    byte_count += (8 + 4 + keyed_seq_msg.baggage.len()) as u64;
                    match s.sample_info().source_timestamp() {
                      Some(ts) => {
                        let now = Timestamp::now();
                        let rtt = now - ts;
                        rtt_total = rtt + rtt_total;
                        rtt_max = std::cmp::max(rtt_max, rtt);
                      }
                      None => println!("Pong without source timestamp!"),
                    }
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
    } // Ping

    MainMode::Pong => {
      let subscriber = domain_participant.create_subscriber(&qos).unwrap();
      let data_reader = subscriber
        .create_datareader_cdr::<KeyedSeq>(&ping_topic, None) // None = get qos policy from publisher
        .unwrap();
      let publisher = domain_participant.create_publisher(&qos).unwrap();
      let data_writer = publisher
        .create_datawriter_cdr::<KeyedSeq>(&pong_topic, None) // None = get qos policy from publisher
        .unwrap();

      smol::block_on(async {
        let mut sample_stream = data_reader.async_sample_stream();
        let mut event_stream = sample_stream.async_event_stream();
        let mut ticker = StreamExt::fuse(async_io::Timer::interval(Duration::from_secs(1)));

        let mut sample_count = 0_u64;
        let mut byte_count = 0_u64;

        println!("Waiting for messages.");
        loop {
          futures::select! {

            _tick = ticker.select_next_some() => {
              println!("{sample_count:9} samples {byte_count:9} bytes");
              sample_count = 0;
              byte_count = 0;
            }

            result = sample_stream.select_next_some() => {
              match result {
                Ok(s) => match s.value() {
                  Sample::Value(keyed_seq_msg) => {
                    sample_count += 1;
                    // estimate size of message on the wire:
                    // 8 bytes for u32 + u32
                    // 4 bytes for baggage sequence size
                    byte_count += (8 + 4 + keyed_seq_msg.baggage.len()) as u64;
                    match s.sample_info().source_timestamp() {
                      Some(ts) => {
                        data_writer.async_write(keyed_seq_msg.clone(), Some(ts))
                          .await
                          .unwrap();
                      }
                      None => println!("Ping without source timestamp!"),
                    }
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
    } // Pong
  } // match main_mode
} // fn
