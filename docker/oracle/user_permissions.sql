-- important to select from v$session used by one of tests
GRANT SELECT ANY DICTIONARY TO onetl;

-- same, but for Oracle 12.x plus
ALTER SESSION SET container=FREEPDB1;
GRANT SELECT ANY DICTIONARY TO onetl;
