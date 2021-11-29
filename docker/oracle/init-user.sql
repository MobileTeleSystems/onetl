CREATE TABLESPACE ONETL DATAFILE '/opt/oracle/oradata/XE/onetl.dbf' SIZE 200M REUSE AUTOEXTEND ON MAXSIZE 2000M;
alter session set "_ORACLE_SCRIPT"=true;
CREATE USER ONETL IDENTIFIED BY onetl DEFAULT TABLESPACE ONETL TEMPORARY TABLESPACE TEMP;
grant resource,connect,sysdba to ONETL;
ALTER USER ONETL quota unlimited on ONETL;
exit;