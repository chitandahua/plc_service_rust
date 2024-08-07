use bytes::{Buf, BufMut, BytesMut};
use core::fmt;
use serde::Deserialize;
use serialport::SerialPort;
use std::io::{self, Cursor, Read, Write};
use std::net::TcpStream;
use tracing::{debug, info};

use crate::Result;

#[derive(Debug, Deserialize)]
struct UartConfig {
    port: String,
    #[serde(rename = "baudrate")]
    baud_rate: u32,
    #[serde(rename = "wordlength")]
    word_length: u8,
    parity: String,
    #[serde(rename = "stopbit")]
    stop_bit: u8,
}

pub struct SerialPort {
    pub reader: StreamReader,
    pub writer: StreamWriter,
}

pub struct StreamReader {
    stream: Box<dyn Read + Send>,
    buffer: BytesMut,
}

pub struct StreamWriter {
    stream: Box<dyn Write + Send>,
}

struct SerialPortAdapter {
    port: Box<dyn SerialPort>,
}

impl Read for SerialPortAdapter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.port.read(buf)
    }
}

impl Write for SerialPortAdapter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.port.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.port.flush()
    }
}

impl UartConfig {
    fn open(self) -> serialport::Result<Box<dyn SerialPort>> {
        serialport::new(self.port, self.baudrate)
            .timeout(std::time::Duration::from_millis(100))
            .data_bits(match self.wordlength {
                5 => serialport::DataBits::Five,
                6 => serialport::DataBits::Six,
                7 => serialport::DataBits::Seven,
                8 => serialport::DataBits::Eight,
                _ => serialport::DataBits::Eight,
            })
            .stop_bits(match self.stopbit {
                1 => serialport::StopBits::One,
                2 => serialport::StopBits::Two,
                _ => serialport::StopBits::One,
            })
            .parity(match self.parity.as_str() {
                "n" | "N" => serialport::Parity::None,
                "o" | "O" => serialport::Parity::Odd,
                "e" | "E" => serialport::Parity::Even,
                _ => serialport::Parity::None,
            })
            .flow_control(serialport::FlowControl::None)
            .timeout(std::time::Duration::from_millis(100))
            .open()
    }
}

impl SerialPort {
    pub fn new(uart_config: PathBuf, tcp_addr: Option<SockAddr>) -> Result<SerialPort> {
        let write_stream: Box<dyn Write + Send>;
        let read_stream: Box<dyn Read + Send>;
        if tcp_addr.is_some() {
            let stream = TcpStream::connect(tcp_addr.unwrap())?;
            write_stream = Box::new(stream.try_clone()?);
            read_stream = Box::new(stream);
        } else {
            let config: UartConfig =
                serde_json::from_reader(std::fs::File::open(uart_config)?)?;
            let stream = config.open()?;
            write_stream = Box::new(SerialPortAdapter {
                port: stream.try_clone()?,
            });
            read_stream = Box::new(SerialPortAdapter { port: stream });
            // 直接将Box<dyn SerialPort>转换为Box<dyn Read + Send> // 不可行
            // Rust is not an inheritance-based language, trait B: A means "B requires A" more than "B extends A".
            // https://stackoverflow.com/questions/63239346/how-do-i-transform-a-vecboxdyn-child-into-a-vecboxdyn-base-where-trait?noredirect=1&lq=1
            // https://stackoverflow.com/questions/68856024/how-can-i-downcast-a-dyn-trait-into-another-trait-in-rust
            //write_stream = stream.try_clone()?.downcast::<dyn Write + Send>().unwrap();
            //read_stream = Box::new(*stream) as Box<dyn Read + Send>;
        }

        Ok(SerialPort {
            reader: StreamReader {
                stream: read_stream,
                buffer: BytesMut::with_capacity(4 * 1024),
            },
            writer: StreamWriter {
                stream: write_stream,
            },
        })
    }
}

impl StreamReader {
    pub fn read_response(&mut self) -> Result<Option<AtCmd>> {
        loop {
            let mut buffer = [0; 1024];
            match self.stream.read(&mut buffer)? {
                0 => {
                    if self.buffer.is_empty() {
                        info!("connection close");
                        return Err(ConnectionError::Closed.into());
                    } else {
                        return Err("connection reset by perr".into());
                    }
                }
                n => {
                    self.buffer.put(&buffer[..n]);
                    debug!("read data len {}", n)
                }
            }

            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::read_response(&mut buf)? {
            Some(response) => {
                let len = buf.position() as usize;
                self.buffer.advance(len);
                Ok(Some(response))
            }
            None => Ok(None),
        }
    }
}

impl StreamWriter {
    pub fn write_request(&mut self, at_cmd: &str) -> Result<()> {
        let _ = self.stream.write(at_cmd.as_bytes())?;
        self.stream.flush()?;

        Ok(())
    }
}
