use bytes::{Buf, BufMut, BytesMut};
use serialport::SerialPort;
use std::io::{self, Cursor, Read, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use tracing::{debug, info};

use crate::protocol::Frame;
use crate::{Result, UartConfig};

pub struct UartPort {
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

fn open(config: UartConfig) -> serialport::Result<Box<dyn SerialPort>> {
    serialport::new(config.port, config.baud_rate)
        .timeout(std::time::Duration::from_millis(100))
        .data_bits(match config.word_length {
            5 => serialport::DataBits::Five,
            6 => serialport::DataBits::Six,
            7 => serialport::DataBits::Seven,
            8 => serialport::DataBits::Eight,
            _ => serialport::DataBits::Eight,
        })
        .stop_bits(match config.stop_bit {
            1 => serialport::StopBits::One,
            2 => serialport::StopBits::Two,
            _ => serialport::StopBits::One,
        })
        .parity(match config.parity.as_str() {
            "n" | "N" => serialport::Parity::None,
            "o" | "O" => serialport::Parity::Odd,
            "e" | "E" => serialport::Parity::Even,
            _ => serialport::Parity::None,
        })
        .flow_control(serialport::FlowControl::None)
        .timeout(std::time::Duration::from_millis(100))
        .open()
}

impl UartPort {
    pub fn new(config: UartConfig, tcp_addr: Option<SocketAddr>) -> Result<UartPort> {
        let write_stream: Box<dyn Write + Send>;
        let read_stream: Box<dyn Read + Send>;
        if tcp_addr.is_some() {
            let stream = TcpStream::connect(tcp_addr.unwrap())?;
            write_stream = Box::new(stream.try_clone()?);
            read_stream = Box::new(stream);
        } else {
            let stream = open(config)?;
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

        Ok(UartPort {
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
    pub fn read_response(&mut self) -> Result<Option<Frame>> {
        loop {
            // 可能会有多个帧
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            let mut buffer = [0; 1024];
            match self.stream.read(&mut buffer)? {
                0 => {
                    if self.buffer.is_empty() {
                        info!("connection close");
                        return Err(anyhow::anyhow!("connection close"));
                    } else {
                        return Err(anyhow::anyhow!("connection reset by perr"));
                    }
                }
                n => {
                    self.buffer.put(&buffer[..n]);
                    debug!("read data({}): {}", n, hex::encode(&buffer[..n]))
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::parse(&mut buf)? {
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
    pub fn write_request(&mut self, req: impl AsRef<[u8]>) -> Result<()> {
        let _ = self.stream.write(req.as_ref())?;
        self.stream.flush()?;

        Ok(())
    }
}
