use [NuGet_Backup_20130417002350-2013-4-16-17-35]

-- tables to be pulled into the graph
-- PackageRegistrations			-> Package nodes
-- Packages						-> Package version nodes
-- PackageAuthors				-> Person nodes + authored_by relationships (these need to be de-duped as there are currently multiple rows for the same logical person)
--									for creating the rels on the de-duped data set, add entityType column for 'author' and concat all of the ids together for a name
--									the query can then be a 'like' query
-- PackageDependencies			-> depends_on relationships
-- PackageRegistrationOwners	-> owned_by relationships
-- Users						-> Person nodes

-- PackageFrameworks (need to see if this can be flattened into an array and stored with the PackageVersion node)

-- ==============================
-- Util
-- ==============================

-- get the columns from different tables and generate the creation script
/*
select
	case when CHARACTER_MAXIMUM_LENGTH is null then TABLE_NAME + '_' + COLUMN_NAME + ' ' + DATA_TYPE + ' NULL'  
	else TABLE_NAME + '_' + COLUMN_NAME + ' ' + DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) + ') NULL'
	end as q, 
	TABLE_NAME 
from INFORMATION_SCHEMA.COLUMNS 
where TABLE_NAME in ('PackageRegistrations', 'Packages', 'PackageAuthors', 'Users')
order by TABLE_NAME desc
*/

drop table GRAPH_NODES

go

drop table GRAPH_RELS

go

-- =================================
-- Create Nodes Table
-- =================================
-- select count(*) from GRAPH_NODES

create table GRAPH_NODES( 
	ID				int	identity(1,1)	NOT NULL	primary key,
	EntityType		varchar(20)			NOT NULL,
	LocalTableKey	int					NOT NULL,
---combined columns ------------------
	Combined_Downloads int NULL,

---table-specific columns ------------
	Users_Username nvarchar(64) NULL,

	Packages_Copyright nvarchar(max) NULL,
	Packages_Created datetime NULL,
	Packages_Description nvarchar(max) NULL,
	Packages_ExternalPackageUrl nvarchar(max) NULL,
	Packages_HashAlgorithm nvarchar(10) NULL,
	Packages_Hash nvarchar(256) NULL,
	Packages_IconUrl nvarchar(max) NULL,
	Packages_IsLatest bit NULL,
	Packages_LastUpdated datetime NULL,
	Packages_LicenseUrl nvarchar(max) NULL,
	Packages_Published datetime NULL,
	Packages_PackageFileSize bigint NULL,
	Packages_ProjectUrl nvarchar(max) NULL,
	Packages_RequiresLicenseAcceptance bit NULL,
	Packages_Summary nvarchar(max) NULL,
	Packages_Tags nvarchar(max) NULL,
	Packages_Title nvarchar(256) NULL,
	Packages_Version nvarchar(64) NULL,
	Packages_FlattenedAuthors nvarchar(max) NULL,
	Packages_FlattenedDependencies nvarchar(max) NULL,
	Packages_IsLatestStable bit NULL,
	Packages_Listed bit NULL,
	Packages_IsPrerelease bit NULL,
	Packages_ReleaseNotes nvarchar(max) NULL,
	Packages_Language nvarchar(20) NULL,
	Packages_MinClientVersion nvarchar(44) NULL,

	PackageRegistrations_Id nvarchar(128) NULL,

	PackageAuthors_Name nvarchar(max) NULL
)

go

-- ===============================================
-- copy data over from source table to nodes table
-- ===============================================

-- Users
insert into GRAPH_NODES
	(EntityType,LocalTableKey, Users_Username)
select
	'user',
	u.[Key],
	u.Username
from Users u

go

-- Package Registrations
insert into GRAPH_NODES
	(EntityType, LocalTableKey,
	Combined_Downloads,
	PackageRegistrations_Id)
select
	'package_registration',
	r.[Key],
	r.DownloadCount, r.Id
from PackageRegistrations r

go

-- Packages
insert into GRAPH_NODES
	(EntityType, LocalTableKey,
	Packages_Copyright, 
	Packages_Created,
	Packages_Description,
	Combined_Downloads,
	Packages_ExternalPackageUrl,
	Packages_FlattenedAuthors,
	Packages_FlattenedDependencies,
	Packages_Hash,
	Packages_HashAlgorithm,
	Packages_IconUrl,
	Packages_IsLatest,
	Packages_IsLatestStable,
	Packages_IsPrerelease,
	Packages_Language,
	Packages_LastUpdated,
	Packages_LicenseUrl,
	Packages_Listed,
	Packages_MinClientVersion,
	Packages_PackageFileSize,
	Packages_ProjectUrl,
	Packages_Published,
	Packages_ReleaseNotes,
	Packages_RequiresLicenseAcceptance,
	Packages_Summary,
	Packages_Tags,
	Packages_Title,
	Packages_Version)
select 
	'package',
	p.[Key],
	p.Copyright, 
	p.Created,
	p.Description,
	p.DownloadCount,
	p.ExternalPackageUrl,
	p.FlattenedAuthors,
	p.FlattenedDependencies,
	p.Hash,
	p.HashAlgorithm,
	p.IconUrl,
	p.IsLatest,
	p.IsLatestStable,
	p.IsPrerelease,
	p.Language,
	p.LastUpdated,
	p.LicenseUrl,
	p.Listed,
	p.MinClientVersion,
	p.PackageFileSize,
	p.ProjectUrl,
	p.Published,
	p.ReleaseNotes,
	p.RequiresLicenseAcceptance,
	p.Summary,
	p.Tags,
	p.Title,
	p.Version
from Packages p

go

-- Package Authors
insert into GRAPH_NODES
	(EntityType, LocalTableKey,
	PackageAuthors_Name)
select 
	'author',
	a.[Key],
	a.Name
from PackageAuthors a

go

-- ==============================
-- Create relationships table
-- ==============================

create table GRAPH_RELS
(
	start				int				not null,
	[end]				int				not null,
	[type]				nvarchar(128)	not null,
	VersionSpec			nvarchar(256)	null,	-- PackageDependencies
	TargetFramework		nvarchar(256)	null,	-- PackageDependencies
)

go

-- =============================
-- Create relationships
-- =============================
-- select count(*) from GRAPH_RELS

-- Package -[:version_of]-> PackageRegistration
insert into GRAPH_RELS
	(start, [end], [type])
select
	(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=p.[Key] and n.EntityType='package') start,
	(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=p.PackageRegistrationKey and n.EntityType='package_registration') [end],
	'version_of' [type]
from Packages p

go

-- Package -[:depends_on]-> PackageRegistration
insert into GRAPH_RELS
	(start, [end], [type], VersionSpec, TargetFramework)
-- NOTE: this is a lossy operation (1047 rows of source data) due to the fact that some rows in the PackageDependencies table 
--		 refer to package registrations that don't actually exist.
select * from (
	select
		(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=d.PackageKey and n.EntityType='package') start,
		(select top 1 ID from GRAPH_NODES n where n.PackageRegistrations_Id=d.Id and n.EntityType='package_registration') [end],
		'depends_on' [type],
		d.VersionSpec,
		d.TargetFramework
	from PackageDependencies d) s
where s.[end] is not null

go

-- PackageRegistration -[:owned_by]-> User
insert into GRAPH_RELS
	(start, [end], [type])
select
	(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=o.PackageRegistrationKey and n.EntityType='package_registration') start,
	(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=o.UserKey and n.EntityType='user') [end],
	'owned_by' [type]
from PackageRegistrationOwners o

go

-- Package -[:authored_by]-> PackageAuthor
insert into GRAPH_RELS
	(start, [end], [type])
select
	(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=a.PackageKey and n.EntityType='package') start,
	(select top 1 ID from GRAPH_NODES n where n.LocalTableKey=a.[Key] and n.EntityType='author') [end],
	'authored_by' [type]
from PackageAuthors a

go

-- ===============================
-- Queries for the CSV Files
-- ===============================
-- select 'n.' + COLUMN_NAME + ',' from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'GRAPH_NODES'
-- select 'r.' + COLUMN_NAME + ',' from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'GRAPH_RELS'

-- nodes.csv
-- header query:
-- select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'GRAPH_NODES'
-- type	username	copyright	packageCreated	description	downloads	externalPackageUrl	hashAlgorithm	hash	iconUrl	isLatest	updated	licenseUrl	published	size	projectUrl	requiresLicenseAcceptance	summary	tags	title	version	flattenedAuthors	flattenedDependencies	isLatestStable	isListed	isPrerelease	releaseNotes	language	minClientVersion	packageId	name

select 
	ISNULL(n.EntityType,'')																	as [type],
	ISNULL(REPLACE(REPLACE(n.Users_Username, CHAR(13), ''), CHAR(10), ''),'')					as [username],
	ISNULL(REPLACE(REPLACE(n.Packages_Copyright, CHAR(13), ''), CHAR(10), ''),'')			as copyright,
	ISNULL(CAST(n.Packages_Created as nvarchar(50)),'')															as created,
	ISNULL(REPLACE(REPLACE(n.Packages_Description, CHAR(13), ''), CHAR(10), ''),'')			as [description],
	ISNULL(CAST(n.Combined_Downloads as nvarchar(50)),'')															as downloads,
	ISNULL(n.Packages_ExternalPackageUrl,'')												as externalPackageUrl,
	ISNULL(n.Packages_HashAlgorithm,'')														as hashAlgorithm,
	ISNULL(n.Packages_Hash,'')																as [hash],
	ISNULL(n.Packages_IconUrl,'')															as iconUrl,
	ISNULL(CAST(n.Packages_IsLatest as nvarchar(10)),'')															as isLatest,
	ISNULL(CAST(n.Packages_LastUpdated as nvarchar(50)),'')														as updated,
	ISNULL(n.Packages_LicenseUrl,'')														as licenseUrl,
	ISNULL(CAST(n.Packages_Published as nvarchar(50)),'')															as published,
	ISNULL(CAST(n.Packages_PackageFileSize as nvarchar(25)),'')													as size,
	ISNULL(n.Packages_ProjectUrl,'')														as projectUrl,
	ISNULL(CAST(n.Packages_RequiresLicenseAcceptance as nvarchar(10)),'')											as requiresLicenseAcceptance,
	ISNULL(REPLACE(REPLACE(n.Packages_Summary, CHAR(13), ''), CHAR(10), ''),'')				as summary,
	ISNULL(REPLACE(REPLACE(n.Packages_Tags, CHAR(13), ''), CHAR(10), ''),'')				as tags,
	ISNULL(REPLACE(REPLACE(n.Packages_Title, CHAR(13), ''), CHAR(10), ''),'')				as title,
	ISNULL(n.Packages_Version,'')															as [version],
	ISNULL(n.Packages_FlattenedAuthors,'')													as flattenedAuthors,
	ISNULL(n.Packages_FlattenedDependencies,'')												as flattenedDependencies,
	ISNULL(CAST(n.Packages_IsLatestStable as nvarchar(10)),'')													as isLatestStable,
	ISNULL(CAST(n.Packages_Listed as nvarchar(10)),'')															as isListed,
	ISNULL(CAST(n.Packages_IsPrerelease as nvarchar(10)),'')														as isPrerelease,
	ISNULL(REPLACE(REPLACE(n.Packages_ReleaseNotes, CHAR(13), ''), CHAR(10), ''),'')		as releaseNotes,
	ISNULL(n.Packages_Language,'')															as [language],
	ISNULL(n.Packages_MinClientVersion,'')													as minClientVersion,
	ISNULL(REPLACE(REPLACE(n.PackageRegistrations_Id, CHAR(13), ''), CHAR(10), ''),'')		as packageId,
	ISNULL(REPLACE(REPLACE(n.PackageAuthors_Name, CHAR(13), ''), CHAR(10), ''),'')			as name
from GRAPH_NODES n

go 

-- rels.csv
-- header query:
-- select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'GRAPH_RELS'
-- start	end	type	versionSpec	targetFramework
select 
	r.start							as start,
	r.[end]							as [end],
	r.[type]						as [type],
	ISNULL(r.VersionSpec,'')		as versionSpec,
	ISNULL(r.TargetFramework,'')	as targetFramework
from GRAPH_RELS r

go 

-- entities_idx.csv
-- entity types: author, package, package_registration, user
-- id	type	name
select * from 
(select 
	n.ID as [id],
	n.EntityType as [type],
	case
		when n.EntityType='package_registration' then lower(REPLACE(REPLACE(n.PackageRegistrations_Id, CHAR(13), ''), CHAR(10), ''))
		when n.EntityType='author' then lower(REPLACE(REPLACE(n.PackageAuthors_Name, CHAR(13), ''), CHAR(10), ''))
		when n.EntityType='package' then lower(REPLACE(REPLACE(n.Packages_Title, CHAR(13), ''), CHAR(10), ''))
		when n.EntityType='user' then lower(REPLACE(REPLACE(n.Users_Username, CHAR(13), ''), CHAR(10), ''))
	end name
from GRAPH_NODES n) sub
where sub.name is not null

