#!/usr/bin/env bash
set -e

# https://serverfault.com/questions/157159/too-many-ftp-connection-causing-error-421
echo "max_per_ip=0" >> /etc/vsftpd/vsftpd.conf

# https://serverfault.com/questions/65002/vsftpd-and-implicit-ssl
echo "implicit_ssl=NO" >> /etc/vsftpd/vsftpd.conf

# enable anonymous login for both FTP and FTPS
echo "anonymous_enable=YES" >> /etc/vsftpd/vsftpd.conf
echo "allow_anon_ssl=YES" >> /etc/vsftpd/vsftpd.conf
