alter table migrationmetadata add rowCntDest bigint not null default 0;
alter table migrationmetadata add dbname nvarchar(255);
alter table migrationmetadata add dataset nvarchar(255);
alter table migrationmetadata add tag1 nvarchar(255);
alter table migrationmetadata add tag2 nvarchar(255);
alter table migrationmetadata add tag3 nvarchar(255);
alter table migrationmetadata add tagint bigint not null default 0;
alter table migrationmetadata add cntDate datetime2(7);