-- increase number of processes on XE:
-- see https://dba.stackexchange.com/questions/110819/oracle-intermittently-throws-ora-12516-tnslistener-could-not-find-available-h
ALTER SYSTEM SET processes=150 scope=spfile;
SHUTDOWN IMMEDIATE;
STARTUP;
