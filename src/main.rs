use anyhow::{Result, bail};
use network_interface::NetworkInterfaceConfig as _;
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    str::FromStr,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt as _, Interest},
    net::TcpStream,
};

fn name_to_scope(name: &str) -> Result<u32> {
    let ifs = network_interface::NetworkInterface::show()?;

    for i in ifs {
        if let Some(a) = i.addr {
            if a.ip().is_loopback() {
                continue;
            }

            if !a.ip().is_ipv6() {
                continue;
            }

            if i.name == name {
                return Ok(i.index);
            }
        }
    }

    bail!("do not know interface {name:?}");
}

fn scope_to_name(scope: u32) -> String {
    let ifs = match network_interface::NetworkInterface::show() {
        Ok(ifs) => ifs,
        Err(e) => return format!("<?ERR:{e}>"),
    };

    for i in ifs {
        if i.index != scope {
            continue;
        }

        if let Some(a) = i.addr {
            if a.ip().is_loopback() {
                continue;
            }

            if !a.ip().is_ipv6() {
                continue;
            }

            return i.name.to_string();
        }
    }

    format!("scope:{scope}")
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    let a = getopts::Options::new()
        .parsing_style(getopts::ParsingStyle::StopAtFirstFree)
        .parse(std::env::args_os().skip(1))?;

    match a.free.get(0).map(String::as_str) {
        Some("listen") => {
            if a.free.len() != 2 {
                bail!("want listen port");
            }

            return listen(a.free[1].parse()?).await;
        }
        Some("connect") => {
            if a.free.len() != 4 {
                bail!("want: interface, port, IP");
            }

            let scope = name_to_scope(&a.free[1])?;
            let port: u16 = a.free[2].parse()?;
            let ip = Ipv6Addr::from_str(&a.free[3])?;

            return connect(scope, port, ip).await;
        }
        Some("ifs") => {
            if a.free.len() != 1 {
                bail!("what");
            }

            let ifs = network_interface::NetworkInterface::show()?;

            for i in ifs {
                if let Some(a) = i.addr {
                    if a.ip().is_loopback() {
                        continue;
                    }

                    if !a.ip().is_ipv6() {
                        continue;
                    }

                    println!(
                        "{} idx {} addr {}",
                        i.name,
                        i.index,
                        a.ip().to_string()
                    );
                }
            }
        }
        Some(other) => bail!("don't know about {other:?}"),
        None => bail!("what command?"),
    }

    Ok(())
}

async fn listen(port: u16) -> Result<()> {
    println!("listening on port {port}...");

    let la = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0);

    let mut next_id = 1u64;
    let lis = tokio::net::TcpListener::bind(la).await?;
    loop {
        let (conn, whom) = lis.accept().await?;

        let id = next_id;
        next_id += 1;

        match whom {
            std::net::SocketAddr::V4(_) => continue,
            std::net::SocketAddr::V6(sa) => {
                tokio::spawn(async move {
                    proc_noerr(id, conn, sa).await;
                });
            }
        }
    }
}

async fn proc_noerr(id: u64, conn: TcpStream, whom: SocketAddrV6) {
    println!(
        "connection {id} from {whom} via {}",
        scope_to_name(whom.scope_id()),
    );
    if let Err(e) = proc(id, conn).await {
        println!("{id} failed: {e}");
        return;
    }

    println!("{id} ended");
}

async fn proc(id: u64, mut conn: TcpStream) -> Result<()> {
    conn.set_nodelay(true)?;

    let mut buf = vec![0u8; 1];

    let mut deadline =
        Instant::now().checked_add(Duration::from_secs(5)).unwrap();

    loop {
        let res =
            match tokio::time::timeout_at(deadline.into(), conn.read(&mut buf))
                .await
            {
                Ok(res) => {
                    deadline = Instant::now()
                        .checked_add(Duration::from_secs(5))
                        .unwrap();
                    res
                }
                Err(e) => bail!("I/O timed out: {e}"),
            };

        match res {
            Ok(0) => {
                println!("{id}: EOF on read");
                return Ok(());
            }
            Ok(sz) => {
                conn.write_all(&buf[0..sz]).await?;
            }
            Err(e) => bail!("read error: {e}"),
        };
    }
}

async fn connect(scope: u32, port: u16, ip: Ipv6Addr) -> Result<()> {
    let mut next_id = 1u64;

    loop {
        let id = next_id;
        next_id += 1;

        match connect_one(id, scope, port, ip).await {
            Ok(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                eprintln!("{id}: ERROR: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn connect_one(
    id: u64,
    scope: u32,
    port: u16,
    ip: Ipv6Addr,
) -> Result<()> {
    let ca = SocketAddrV6::new(ip, port, 0, scope);

    println!("{id}: connecting to {ca} via {}...", scope_to_name(scope));

    let start = Instant::now();
    let mut conn = tokio::net::TcpStream::connect(ca).await?;
    let dur = Instant::now().duration_since(start);
    println!("{id}: connected after {} msec", dur.as_millis());

    let mut outstanding = None;
    let mut deadline =
        Instant::now().checked_add(Duration::from_secs(5)).unwrap();
    let mut zeroes = 0;

    conn.set_nodelay(true)?;

    loop {
        let mut interest = Interest::READABLE;
        if outstanding.is_none() {
            interest |= Interest::WRITABLE;
        }

        let fut = conn.ready(interest);

        let res = match tokio::time::timeout_at(deadline.into(), fut).await {
            Ok(res) => res,
            Err(e) => {
                bail!("I/O timed out: {e}");
            }
        };

        match res {
            Ok(r) => {
                if outstanding.is_none() && interest.is_writable() {
                    match conn.try_write(b"A") {
                        Ok(_) => {
                            outstanding = Some(Instant::now());
                        }
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            ()
                        }
                        Err(e) => bail!("write error: {e}"),
                    }
                }

                if interest.is_readable() {
                    let mut buf = vec![0u8; 1];

                    match conn.try_read(&mut buf) {
                        Ok(0) => {
                            println!("{id}: EOF on read");
                            return Ok(());
                        }
                        Ok(1) => {
                            if buf[0] == b'A' {
                                if let Some(sent) = outstanding.take() {
                                    let msec = Instant::now()
                                        .saturating_duration_since(sent)
                                        .as_millis();
                                    if msec == 0 {
                                        zeroes += 1;
                                    } else {
                                        println!(
                                            "{id}: rtt {msec} msec \
                                            (after {zeroes} zeroes)"
                                        );
                                        zeroes = 0;
                                    }
                                    tokio::time::sleep(Duration::from_millis(
                                        100,
                                    ))
                                    .await;

                                    deadline = Instant::now()
                                        .checked_add(Duration::from_secs(5))
                                        .unwrap();
                                } else {
                                    bail!("did not expect reply traffic");
                                }
                            } else {
                                bail!("incorrect reply traffic");
                            }
                        }
                        Ok(sz) => {
                            bail!("unexpected read of {sz} bytes");
                        }
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            ()
                        }
                        Err(e) => bail!("read error: {e}"),
                    };
                }

                if r.is_error() {
                    bail!("error on connection");
                }
            }
            Err(e) => bail!("error on connection: {e}"),
        }
    }
}
