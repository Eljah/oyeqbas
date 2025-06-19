# Netty SOCKS Proxy

This project contains a simple SOCKS5 proxy based on Netty. After fixing the `pom.xml`
configuration it builds and starts correctly.

## Why pages fail to load

When running in the Codex environment the proxy fails to open
connections to external hosts. The outbound network is restricted so any direct
TCP connection results in the error `Network is unreachable`. Because the proxy
tries to connect directly, every client request eventually fails.

To use the proxy successfully you need to run it in an environment with
unrestricted outbound access or modify the code to tunnel outgoing connections
through an allowed HTTP proxy.

## Debugging connection failures

If pages fail to load even though the proxy starts, verify that outbound
connections are permitted. Use `sudo ufw status` or `iptables -L` to inspect
local firewall rules. Even with open firewall ports, this environment blocks
direct TCP connections to the Internet. `curl --socks5-hostname localhost:1080`
can confirm the proxy works when outbound access is allowed.
