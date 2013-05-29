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
select
	case when CHARACTER_MAXIMUM_LENGTH is null then TABLE_NAME + '_' + COLUMN_NAME + ' ' + DATA_TYPE + ' NULL'  
	else TABLE_NAME + '_' + COLUMN_NAME + ' ' + DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) + ') NULL'
	end as q, 
	TABLE_NAME 
from INFORMATION_SCHEMA.COLUMNS 
where TABLE_NAME in ('PackageRegistrations', 'Packages', 'PackageAuthors', 'Users')
order by TABLE_NAME desc


-- =================================
-- Create Nodes Table
-- =================================
-- drop table GRAPH_NODES
-- select count(*) from GRAPH_NODES

create table GRAPH_NODES( 
	ID				int	identity(1,1)	NOT NULL	primary key,
	EntityType		varchar(20)			NOT NULL,
	LocalTableKey	int					NOT NULL,
---combined columns ------------
	Users_Username nvarchar(64) NULL,

	Packages_Copyright nvarchar(max) NULL,
	Packages_Created datetime NULL,
	Packages_Description nvarchar(max) NULL,
	Packages_DownloadCount int NULL,
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
	PackageRegistrations_DownloadCount int NULL,

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
	PackageRegistrations_DownloadCount,
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
	Packages_DownloadCount,
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
-- drop table GRAPH_RELS

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
-- ID	EntityType	LocalTableKey	Users_Username	Packages_Copyright	Packages_Created	Packages_Description	Packages_DownloadCount	Packages_ExternalPackageUrl	Packages_HashAlgorithm	Packages_Hash	Packages_IconUrl	Packages_IsLatest	Packages_LastUpdated	Packages_LicenseUrl	Packages_Published	Packages_PackageFileSize	Packages_ProjectUrl	Packages_RequiresLicenseAcceptance	Packages_Summary	Packages_Tags	Packages_Title	Packages_Version	Packages_FlattenedAuthors	Packages_FlattenedDependencies	Packages_IsLatestStable	Packages_Listed	Packages_IsPrerelease	Packages_ReleaseNotes	Packages_Language	Packages_MinClientVersion	PackageRegistrations_Id	PackageRegistrations_DownloadCount	PackageAuthors_Name

select 
	n.EntityType,
	n.LocalTableKey,
	n.Users_Username,
	n.Packages_Copyright,
	n.Packages_Created,
	n.Packages_Description,
	n.Packages_DownloadCount,
	n.Packages_ExternalPackageUrl,
	n.Packages_HashAlgorithm,
	n.Packages_Hash,
	n.Packages_IconUrl,
	n.Packages_IsLatest,
	n.Packages_LastUpdated,
	n.Packages_LicenseUrl,
	n.Packages_Published,
	n.Packages_PackageFileSize,
	n.Packages_ProjectUrl,
	n.Packages_RequiresLicenseAcceptance,
	n.Packages_Summary,
	n.Packages_Tags,
	n.Packages_Title,
	n.Packages_Version,
	n.Packages_FlattenedAuthors,
	n.Packages_FlattenedDependencies,
	n.Packages_IsLatestStable,
	n.Packages_Listed,
	n.Packages_IsPrerelease,
	n.Packages_ReleaseNotes,
	n.Packages_Language,
	n.Packages_MinClientVersion,
	n.PackageRegistrations_Id,
	n.PackageRegistrations_DownloadCount,
	n.PackageAuthors_Name
from GRAPH_NODES n

-- rels.csv
-- header query:
-- select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'GRAPH_RELS'
-- start	end	type	VersionSpec	TargetFramework
select 
	r.start,
	r.[end],
	r.[type],
	r.VersionSpec,
	r.TargetFramework
from GRAPH_RELS r

-- entities_idx.csv
-- entity types: author, package, package_registration, user
-- header: id	type	name
select 
	n.ID,
	n.EntityType,
	case
		when n.EntityType='package_registration' then n.PackageRegistrations_Id
		when n.EntityType='author' then n.PackageAuthors_Name
		when n.EntityType='package' then n.Packages_Title
		when n.EntityType='user' then n.Users_Username
	end EntityName
from GRAPH_NODES n

