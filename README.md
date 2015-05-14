# FTP file input plugin for Embulk

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **host**: FTP server address (string, required)
- **port**: FTP server port number (integer, default: `21`. `990` if `ssl` is true)
- **user**: user name to login (string, optional)
- **password**: password to login (string, default: `""`)
- **path_prefix** prefix of target files (string, required)
- **passive_mode**: use passive mode (boolean, default: true)
- **ascii_mode**: use ASCII mode instead of binary mode (boolean, default: false)
- **ssl**: use FTPS (SSL encryption). (boolean, default: false)
- **ssl_no_verify**: disable server certification verification. By default, connection fails if the server certification is not signed by one the CAs in JVM's default trusted CA list. (boolean, default: false)
- **ssl_verify_hostname**: verify server's hostname matches with provided certificate. (boolean, default: true)
- **ssl_trusted_ca_cert_file**: if the server certification is not signed by a certificate authority, set path to the X.508 certification file (pem file) of a private CA (string, optional)
- **ssl_trusted_ca_cert_data**: similar to `ssl_trusted_ca_cert_file` but embed the contents of the PEM file as a string value instead of path to a local file (string, optional)

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

With SSL:

```yaml
in:
  type: ftp
  host: ftp.example.net
  port: 21
  user: anonymous
  password: "mypassword"
  path_prefix: /ftp/file/path/prefix

  ssl: true

  #ssl_no_verify: true    # to disable server certificate verification

  # if you use self-signed certificate, embed the PEM data
  ssl_trusted_ca_cert_data: |
      -----BEGIN CERTIFICATE-----
      MIIFV...
      ...
      ...
      -----END CERTIFICATE-----

  # or set path to the pem file
  ssl_trusted_ca_cert_file: /path/to/ca_cert.pem
```

## Build

```
$ ./gradlew gem
```
