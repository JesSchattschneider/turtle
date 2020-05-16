-- Table: tags.conf

-- DROP TABLE tags.histo_temp;
-- precisa criar uma chave primaria?
-- IMPORTA TDS OS DADOS DA TABELA, HA DADOS DUPLICADOS

Drop table if exists tags.histo_temp cascade;
CREATE TABLE tags.histo_temp
(
  id_no integer,
  ptt integer,
  depthsensor varchar (50),
  source varchar (50),
  instr varchar (50),
  hyst_type varchar (50),
  utc varchar(50),
  offs integer,
  count integer,
  badtherm integer,
  locationquality char(1),
  lat character varying(100),
  lon character varying(100),
  numbins integer,
  sum integer,
  bin_01 numeric(5,2),
  bin_02 numeric(5,2),
  bin_03 numeric(5,2),
  bin_04 numeric(5,2),
  bin_05 numeric(5,2),
  bin_06 numeric(5,2),
  bin_07 numeric(5,2),
  bin_08 numeric(5,2),
  bin_09 numeric(5,2),
  bin_10 numeric(5,2),
  bin_11 numeric(5,2),
  bin_12 numeric(5,2),
  bin_13 numeric(5,2),
  bin_14 numeric(5,2),
  bin_15 numeric(5,2),
  bin_16 numeric(5,2),
  bin_17 numeric(5,2),
  bin_18 numeric(5,2),
  bin_19 numeric(5,2),
  bin_20 numeric(5,2),
  bin_21 numeric(5,2),
  bin_22 numeric(5,2),
  bin_23 numeric(5,2),
  bin_24 numeric(5,2),
  bin_25 numeric(5,2),
  bin_26 numeric(5,2),
  bin_27 numeric(5,2),
  bin_28 numeric(5,2),
  bin_29 numeric(5,2),
  bin_30 numeric(5,2),
  bin_31 numeric(5,2),
  bin_32 numeric(5,2),
  bin_33 numeric(5,2),
  bin_34 numeric(5,2),
  bin_35 numeric(5,2),
  bin_36 numeric(5,2),
  bin_37 numeric(5,2),
  bin_38 numeric(5,2),
  bin_39 numeric(5,2),
  bin_40 numeric(5,2),
  bin_41 numeric(5,2),
  bin_42 numeric(5,2),
  bin_43 numeric(5,2),
  bin_44 numeric(5,2),
  bin_45 numeric(5,2),
  bin_46 numeric(5,2),
  bin_47 numeric(5,2),
  bin_48 numeric(5,2),
  bin_49 numeric(5,2),
  bin_50 numeric(5,2),
  bin_51 numeric(5,2),
  bin_52 numeric(5,2),
  bin_53 numeric(5,2),
  bin_54 numeric(5,2),
  bin_55 numeric(5,2),
  bin_56 numeric(5,2),
  bin_57 numeric(5,2),
  bin_58 numeric(5,2),
  bin_59 numeric(5,2),
  bin_60 numeric(5,2),
  bin_61 numeric(5,2),
  bin_62 numeric(5,2),
  bin_63 numeric(5,2),
  bin_64 numeric(5,2),
  bin_65 numeric(5,2),
  bin_66 numeric(5,2),
  bin_67 numeric(5,2),
  bin_68 numeric(5,2),
  bin_69 numeric(5,2),
  bin_70 numeric(5,2),
  bin_71 numeric(5,2),
  bin_72 numeric(5,2)
);


--Importando tabelas processadas pelo DAP processor
--Abrir o csv com libreoffice, substituir vazios por nan
--Deletar linhas vazias
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150420-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150409-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150312-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150318_mk10-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150401-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150409-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150420-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSC_20150506-Histos3.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150521_2-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150609-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/ArgosData_2015_07_08-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSC_2015_07_18-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSC_20150807-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSC_20150701-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/DSA_20150609-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/histo/DSC_20150910-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/histo/DSC_20150930-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/histo/DSC_20151010-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/20151027_histo2.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/20151117_histo.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/20151229_histos.csv' csv header delimiter ',' null as 'nan';

--diretorio no MAC:

--EXPORTAR TABELAS
--\copy telemetriafbat to '/Users/jessicaschattschneider/Dropbox/tables_pg_matlab/telemetriafbat.csv' with csv header delimiter ',' null as 'nan';

\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150420-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150409-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150312-Histos_bd.csv' csv header delimiter ',' null as 'nan';

\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150318_mk10-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150401-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150409-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150420-Histos_bd.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA-Histos_bd.csv' csv header delimiter ',' null as 'nan';

\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSC_20150506-Histos3.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150521_2-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150609-Histos_bd.csv' csv header delimiter ',' null as 'nan';

\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/DSC_20150701-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/ArgosData_2015_07_08-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/DSC_2015_07_18-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/DSC_20150807-Histos2.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/DSC_20150826-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/histo/DSC_20150910-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/histo/DSC_20150930-Histos.csv' csv header delimiter ',' null as 'nan';

\copy histo_temp from '/Users/anale/Dropbox/ARGOS (1)/20150415_processados/DSC_20151117-Histos.csv' csv header delimiter ',' null as 'nan';


\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20150609-Histos_bd.csv' csv header delimiter ',' null as 'nan';

\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/histo/DSC_20151010-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/20151027_histo2.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/20151117_histo.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/20151229_histos.csv' csv header delimiter ',' null as 'nan';

ALTER TABLE tags.histo_temp drop COLUMN key_column;

\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20160108-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20160120-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20160210-Histos.csv' csv header delimiter ',' null as 'nan';
\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20160215-Histos.csv' csv header delimiter ',' null as 'nan';

--25-09-2015 copiar para csv
--\copy (select * From tags.dde) To '//Users/jessicaschattschneider/Dropbox/dde.csv' With CSV;

--Criando a tabela de dados histo final
Drop table if exists tags.histo cascade;
CREATE TABLE tags.histo
( id serial primary key,
  tag_id integer,
  source varchar (50),
  instr varchar (50),
  hyst_type varchar (50),
  utc timestamp,
  offs integer,
  count integer,
  badtherm integer,
  lc char(1),
  lat numeric(11,8),
  lon numeric(11,8),
  numbins integer,
  sum integer,
  bin_01 numeric(5,2),
  bin_02 numeric(5,2),
  bin_03 numeric(5,2),
  bin_04 numeric(5,2),
  bin_05 numeric(5,2),
  bin_06 numeric(5,2),
  bin_07 numeric(5,2),
  bin_08 numeric(5,2),
  bin_09 numeric(5,2),
  bin_10 numeric(5,2),
  bin_11 numeric(5,2),
  bin_12 numeric(5,2),
  bin_13 numeric(5,2),
  bin_14 numeric(5,2)
  );


ALTER TABLE tags.histo_temp ADD COLUMN key_column BIGSERIAL PRIMARY KEY;

ALTER TABLE tags.histo_temp drop COLUMN id key_column;
   UPDATE tags.histo_temp SET id = nextval(pg_get_serial_sequence('tags.histo_temp','id'));
   ALTER TABLE tags.histo_temp ADD PRIMARY KEY (id);
 
  --Inserindo dados da tabela temporária na tabela histo final
insert into tags.histo (tag_id, source, instr, hyst_type, 
	utc, offs, count, badtherm, lc, lat, lon, numbins, sum,
	bin_01, bin_02, bin_03,  bin_04,  bin_05,  bin_06,  bin_07,  bin_08,  bin_09,  bin_10,  
	bin_11,  bin_12,  bin_13, bin_14)
	
	select id_no, source, instr, hyst_type, 
	to_timestamp(utc, 'HH24:MI:SS DD-Mon-YYYY'),
	offs, count, badtherm, locationquality, lat::numeric, lon::numeric, numbins, sum,
	bin_01, bin_02, bin_03,  bin_04,  bin_05,  bin_06,  bin_07,  bin_08,  bin_09,  bin_10,  
	bin_11,  bin_12,  bin_13, bin_14
	from histo_temp;
select * from tags.histo
order by utc;

--Deletando duplicatas no tags.histo
create table jess.histof as 
select * 
FROM histo 
WHERE ctid NOT IN (SELECT max(ctid) FROM histo GROUP BY tag_id,source,instr,hyst_type,utc,offs,count,lc,lat,lon,numbins,sum,bin_01,bin_02,bin_03,bin_04,bin_05,bin_06,bin_07,bin_08,bin_09,bin_10,bin_11,bin_12,bin_13,bin_14);

select count(*) from tags.histo;
select count(*) from jess.histof

--Verificando últimos dados da CRAN

select *
from tags.histo
where tag_id=140029
order by hyst_type, utc desc;


--CRIAR TABELAS PELO TIPO DE DADO DO DIVE PROFILE

--TAT
Drop table if exists tags.TAT cascade;
CREATE TABLE tags.TAT as
SELECT *
FROM tags.histo_temp
WHERE hyst_type = TAT

--TAD
Drop table if exists tags.TAD cascade;
CREATE TABLE tags.TAD
SELECT *
FROM tags.histo_temp
WHERE hyst_type = TAD

--DDE
Drop table if exists tags.DDE cascade;
CREATE TABLE tags.DDE
SELECT *
FROM tags.histo_temp
WHERE hyst_type = DiveDepth


--DDV
Drop table if exists tags.DDU cascade;
CREATE TABLE tags.DDU
SELECT *
FROM tags.histo_temp
WHERE hyst_type = DiveDuration

\copy telemetriaˇ from '/home/jessica/Dropbox/ARGOS (1)/20150415_processados/BD_TABLES/ArgosData_2015_05_21_17_29_32_edit.csv' csv header delimiter ',' null as 'nan';