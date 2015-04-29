# FTP file input plugin for Embulk

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **host**: FTP server address (string, required)
- **port**: FTP server port number (integer, default: 21)
- **user**: user name to login (string, optional)
- **password**: password to login (string, default: `""`)
- **path_prefix** prefix of target keys (string, required)
- **passive_mode**: use passive mode (boolean, default: true)
- **ascii_mode**: use ASCII mode instead of binary mode (boolean, default: false)

## Example

```yaml
in:
  type: ftp
  host: ftp.example.net
  port: 21
  user: anonymous
  password: "mypassword"
  path_prefix: /ftp/file/path/prefix
```

## Build

```
$ ./gradlew gem
```
