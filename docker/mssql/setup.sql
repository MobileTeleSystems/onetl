/*

Enter custom T-SQL here that would run after SQL Server has started up.

*/

CREATE DATABASE onetl;
GO

USE onetl;
GO

CREATE LOGIN onetl WITH PASSWORD = '7ellowEl7akey';
GO

CREATE USER onetl FOR LOGIN onetl;
GO

GRANT CONTROL ON DATABASE::onetl TO onetl;
GO
