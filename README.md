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
- **passive_mode**: use passive mode (integer, default: true)

## Example

```yaml
in:
  type: ftp
  host: my-ftp-server.example.net
  user: mylogin
  password: "mypassword"
  path_prefix: /ftp/file/path/prefix
```

## Build

```
$ ./gradlew gem
```
