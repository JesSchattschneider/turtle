-- Table: tags.conf

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

--diretorio no MAC:

--EXPORTAR TABELAS
--\copy telemetriafbat to '/Users/jessicaschattschneider/Dropbox/tables_pg_matlab/telemetriafbat.csv' with csv header delimiter ',' null as 'nan';

ALTER TABLE tags.histo_temp drop COLUMN key_column;

\copy histo_temp from '/Users/jessicaschattschneider/Dropbox/ARGOS (1)/20150415_processados/DSA_20160108-Histos.csv' csv header delimiter ',' null as 'nan';

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


 1
 2
 3
 4
 5
 6
 7
 8
 9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
64
65
66
67
68
69
70
71
72
73
74
75
76
77
78
79
80
81
82
83
84
85
86
87
88
89
90
--consulta: total de tat, tad, dduration, ddepth, retornados por cada tartaruga
SELECT tag_id, 
       sum((hyst_type='TAT')::int) as tat,
       sum((hyst_type='TAD')::int) as tad,
       sum((hyst_type='DiveDuration')::int) as DiveDuration,
       sum((hyst_type='DiveDepth')::int) as DiveDepth
from tags.histo
group by tag_id
order by tag_id


--CRIAR TABELAS PELO TIPO DE DADO DO DIVE PROFILE
DROP VIEW IF EXISTS tags.tat CASCADE;
create view tags.tat as select *  from histo where hyst_type='TAT';
--select * into tags.tat from histo where hyst_type='tat';

DROP VIEW IF EXISTS tags.tad CASCADE;
create view tags.tad as select *  from histo where hyst_type='TAD';
--select * into tags.tad from histo where hyst_type='tad';

DROP VIEW IF EXISTS tags.dde CASCADE;
create view tags.dde as select *  from histo where hyst_type='DiveDepth';
--select * into tags.dde from histo where hyst_type='DiveDepth';

DROP VIEW IF EXISTS tags.ddu CASCADE;
create view tags.ddu as select *  from histo where hyst_type='DiveDuration';
--select * into tags.ddu from histo where hyst_type='DiveDuration';

-----------------------------------------------------------------------------------------------------------------------

--Consultar quantas datas possuem valores correspondentes de tat e tad
DROP VIEW IF EXISTS tags.datas_tat_tad CASCADE;
create view tags.datas_tat_tad as select tat.tag_id, tat.utc
from tags.tat, tags.tad
where tat.tag_id=tad.tag_id AND tat.utc=tad.utc
group by tat.tag_id, tat.utc
order by tat.tag_id, tat.utc;

--Conta quantas ocorrencias em cada id (tad e tat)
select COUNT(*), datas_tat_tad.tag_id
from tags.datas_tat_tad
group by datas_tat_tad.tag_id
order by datas_tat_tad.tag_id

--Consultar quantas datas possuem valores correspondentes de ddu E dde
DROP VIEW IF EXISTS tags.datas_ddu_dde CASCADE;
create view tags.datas_ddu_dde as select dde.tag_id, dde.utc
from tags.dde, tags.ddu
where dde.tag_id=ddu.tag_id AND dde.utc=ddu.utc
group by dde.tag_id, dde.utc
order by dde.tag_id, dde.utc;

--Conta quantas ocorrencias em cada id (dde e ddu)
select COUNT(*), datas_ddu_dde.tag_id
from tags.datas_ddu_dde
group by datas_ddu_dde.tag_id
order by datas_ddu_dde.tag_id

--Consulta3: quantas datas possuem valores correspondentes para os 4 parametros
DROP VIEW IF EXISTS tags.datas_tot CASCADE;
create view tags.datas_tot as select datas_tat_tad.tag_id, datas_tat_tad.utc
from tags.datas_tat_tad, tags.datas_ddu_dde
where datas_tat_tad.tag_id=datas_ddu_dde.tag_id AND datas_tat_tad.utc=datas_ddu_dde.utc
group by datas_tat_tad.tag_id, datas_tat_tad.utc
order by datas_tat_tad.tag_id, datas_tat_tad.utc;

select COUNT(*), datas_tot.tag_id
from tags.datas_tot
group by datas_tot.tag_id
order by datas_tot.tag_id

-----------------------------------------------------------------------------------------------------------------
--CALCULO DA MEDIA

--frequencia total dos mergulhos realizados por cada tartaruga
select tag_id, sum(sum)
from tags.ddu
group by tag_id
order by tag_id

--select tag_id, bin_01, bin_02, COUNT(bin_01, bin_02)
select tag_id, sum(bin_01), sum(bin_02), sum(bin_03), sum(bin_04), sum(bin_05), sum(bin_06),sum(bin_07), sum(bin_08), sum(bin_09), sum(bin_10), sum(bin_11), sum(bin_12), sum(bin_13), sum(bin_14) 
from tags.ddu
group by tag_id
order by tag_id

DROP VIEW IF EXISTS tags.range CASCADE;
create view tags.range as select *
from tags.conf
where id_no=140027



  1
  2
  3
  4
  5
  6
  7
  8
  9
 10
 11
 12
 13
 14
 15
 16
 17
 18
 19
 20
 21
 22
 23
 24
 25
 26
 27
 28
 29
 30
 31
 32
 33
 34
 35
 36
 37
 38
 39
 40
 41
 42
 43
 44
 45
 46
 47
 48
 49
 50
 51
 52
 53
 54
 55
 56
 57
 58
 59
 60
 61
 62
 63
 64
 65
 66
 67
 68
 69
 70
 71
 72
 73
 74
 75
 76
 77
 78
 79
 80
 81
 82
 83
 84
 85
 86
 87
 88
 89
 90
 91
 92
 93
 94
 95
 96
 97
 98
 99
100
101
102
103
104
105
106
107
108
109
110
111
112
113
114
115
116
117
118
119
120
121
122
123
124
125
126
127
128
129
130
131
132
133
134
135
136
137
138
139
140
141
142
143
144
145
146
147
148
149
150
151
152
153
154
155
156
157
158
159
160
161
162
163
164
165
166
167
168
169
170
171
172
173
174
175
176
177
178
179
180
181
182
183
184
185
186
187
188
189
190
191
192
193
194
195
196
197
198
199
200
201
202
203
204
205
206
207
208
209
210
211
212
213
214
215
216
217
218
219
220
221
222
223
224
225
226
227
228
229
230
231
232
233
234
235
236
237
238
239
240
241
242
243
244
245
246
247
248
249
250
251
252
253
254
255
256
257
258
259
260
261
262
263
264
265
266
267
268
269
270
271
272
273
274
275
276
277
278
279
280
281
282
283
284
285
286
287
288
289
290
291
292
293
294
295
296
297
298
299
300
301
302
303
304
305
306
307
308
309
310
311
312
313
314
315
316
317
318
319
320
321
322
323
324
325
326
327
328
329
330
331
332
333
334
335
336
337
338
339
340
341
342
343
344
345
346
347
348
349
350
351
352
353
354
355
356
357
358
359
360
361
362
363
364
365
366
367
368
369
370
371
372
373
374
375
376
377
---Copia a tabela ttagst no esquema jessica
drop table if exists jess.ttagst cascade;
create table jess.ttagst as
select * from tags.ttagsf 
where tag_id>140000 
--AND lc in ('1','2','3');

select * from jess.ttagst 
limit 10;

--Adiciona valores de SST para a nova tabela
alter table jess.ttagst add column sst double precision;
Update jess.ttagst set sst=st_value(rast,geom)
from imagens.sst
where jess.ttagst.utc::date=sst.utc::date;

--Adiciona valores de GRADIENTE para a nova tabela
alter table jess.ttagst add column grad double precision;
Update jess.ttagst set grad=st_value(rast,geom)
from imagens.grad
where jess.ttagst.utc::date=grad.utc::date;

--Adiciona valores de PROFUNDIDADE para a nova tabela
alter table jess.ttagst add column prof double precision;
Update jess.ttagst set prof=st_value(rast,geom) 
from imagens.gebco
--on t.utc::date=sst.utc::date; --esta condição não se aplica para profundidade.

select * from jess.ttagst limit 10;

--Adiciona valores de DISTANCIA DE FRENTE para a nova tabela
alter table jess.ttagst add column distf numeric(10,3);
update jess.ttagst set distf=
	(select min(st_distance(gradpol.frente020, ttagst.geom,'1'))/1000 
	from imagens.gradpol 
	where jess.ttagst.utc::date=gradpol.utc::date
	group by uid);


--Adiciona SETORES para a nova tabela
alter table jess.ttagst add column sector varchar(100);
update jess.ttagst set sector='Internal Shelf' where prof>-30;
update jess.ttagst set sector='External Shelf' where prof<=-30 and prof>-200;
update jess.ttagst set sector='Slope' where prof<=-200 and prof>-2000;
update jess.ttagst set sector='Plain' where prof<=-2000;

select * from jess.ttagst limit 10
------------------------------------------------------------------------------------------------
--Adiciona a tabela histos para o esquema jessica
drop table if exists jess.histost cascade;
create table jess.histost as
select * from tags.histo
where (extract (year from utc)=2015)
--where lc in ('1','2','3')

------------------------------------------------------------------------------------------------
---------JUNTA DADOS DA HISTOSF COM A TTAGST
--select * from jess.ttagst limit 10;
--menor diferenca de tempo entre histos e ttagsf
drop table if exists jess.mutc cascade;
create view jess.mutc as
select h.tag_id,h.id, min(abs(extract(epoch from (h.utc-t.utc)))) as min-- transforma a data em horas retornando assim um valor numerico para realizar os calculos desejados
from jess.histost h
LEFT JOIN jess.ttagst t ON t.tag_id=h.tag_id
GROUP BY h.id, h.tag_id
order by min
--limit 10

select * from jess.mutc limit 10

--cria table indicando o id de histo e ttagsf os quais possuem a menor diferenca entre seus utcs
drop table if exists jess.inter cascade;
create view jess.inter as
select t.uid,m.*
from jess.mutc m join jess.histost h using (id) 
join jess.ttagst t on (t.tag_id=m.tag_id and min=abs(extract(epoch from (h.utc-t.utc))))


select * from jess.inter order by min

----------------------------------------------------------------------------------------------------------------------------------
--------------TABELA FINAL
--seleciona os dados de interesse de histos e ttagst
drop view if exists jess.histag cascade;
create table jess.histag as
select t.nome_tarta,t.tag_id,h.utc as utch,t.utc as utct,t.lat,t.lon,t.lc,h.hyst_type,h.bin_01 as b01,h.bin_02 as b02,h.bin_03 as b03,h.bin_04 as b04,h.bin_05 as b05,h.bin_06 as b06,
h.bin_07 as b07,h.bin_08 as b08,h.bin_09 as b09,h.bin_10 as b10,h.bin_11 as b11,h.bin_12 as b12,h.bin_13 as b13,h.bin_14 as b14,t.sst,t.grad,t.distf,t.prof,t.sector,i.min/3600 as mutc,t.geom,t.geom_line --em horas
from jess.histost h join jess.inter i using (id)
join jess.ttagst t using (uid)
--order by i.mutc desc


select * from jess.histag limit 10

--ADICIONANDO A CONFIGURAÇÃO APLICADA A CADA TAG:
--alter table jess.histag drop column conf
alter table jess.histag add column conf integer;
update jess.histag set conf=
	(select conf 
	from tags.tag_id 
	where jess.histag.tag_id=tags.tag_id.tag_id
	group by tag_id);

select * from jess.histag 
limit 10;


--ADICIONANDO A ESTAÇÃO:
--alter table jess.histag drop column est
alter table jess.histag add column est varchar(20);
update jess.histag set est=
	(select estacao 
	from public.estacoes
	where extract(month from jess.histag.utch::DATE)=public.estacoes.mes
	group by estacao);

--ADICIONANDO O MES:
--alter table jess.histag drop column mes
alter table jess.histag add column mes integer;
update jess.histag set mes=(select mes from public.estacoes
	where extract(month from jess.histag.utch::DATE)=public.estacoes.mes
	group by mes);

--Separando TIME
--alter table jess.histag drop column timeh
alter table jess.histag add column timeh varchar (20);
update jess.histag set timeh=(jess.histag.utch::TIME);

--Separando DIA
--alter table jess.histag drop column dayh
alter table jess.histag add column dayh varchar (20);
update jess.histag set dayh=(jess.histag.utch::DATE);

select tag_id,utch,hyst_type,geom,count(*) as count
from jess.histag
where tag_id=140027
group by tag_id,utch,hyst_type,geom
order by count desc,utch;

select tag_id, utch, hyst_type, geom
FROM jess.histag WHERE ctid NOT IN
(SELECT max(ctid) FROM jess.histag GROUP BY tag_id,utch,hyst_type,geom)
order by tag_id,utch;

---LIMPANDO
select count(*)
FROM jess.histag WHERE ctid NOT IN
(SELECT max(ctid) FROM jess.histag GROUP BY tag_id,utch,hyst_type)
,geom,b01,b02,b03,b04,b05,b06,b07,b08,b09,b10,b11,b12,b13,b14);

ALTER TABLE jess.histag ADD COLUMN key_column BIGSERIAL PRIMARY KEY;

--Porcentagem de posições menor que 15	
select count(sst)
from jess.ttagst
where ttagst.sst<'15' AND ttagst.sst>='0'

--Quantidade de regu=istros em cada parâmetro
select tag_id,count(*),hyst_type
from jess.histag
group by tag_id,hyst_type
order by tag_id,hyst_type

--Estatisticas do mergulho
--minima sst
select min(sst),max(sst),avg(sst)
from jess.histag
where histag.sst>0 AND histag.sst<30


-----------------------------------VIEWS SEPARANDO OS PARÂMETROS------------------------------------
--TAT
Drop view if exists jess.tat cascade;
CREATE view jess.tat AS
SELECT * FROM jess.histag 
WHERE hyst_type='TAT';

select tag_id, count(*)
from jess.ddu
group by tag_id
order by tag_id

--TAD
Drop view if exists jess.tad cascade;
CREATE view jess.tad as
SELECT * FROM jess.histag
WHERE hyst_type ='TAD'

select * from jess.tad
limit 10

--DDE
Drop view if exists jess.dde cascade;
CREATE view jess.dde as
SELECT * FROM jess.histag
WHERE hyst_type = 'DiveDepth'


--DDU
Drop view if exists jess.ddu cascade;
CREATE view jess.ddu as
SELECT * FROM jess.histag
WHERE hyst_type = 'DiveDuration'

---------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------ Consultas de dados correspondentes----------------------------------------------------------------------------

--consulta: total de TAT, TAD, DDuration, DDepth, retornados por cada tartaruga
SELECT tag_id, 
       sum((hyst_type='TAT')::int) as TAT,
       sum((hyst_type='TAD')::int) as TAD,
       sum((hyst_type='DiveDuration')::int) as DiveDuration,
       sum((hyst_type='DiveDepth')::int) as DiveDepth
from jess.histag
group by tag_id
order by tag_id

select tat.tag_id, count(tat.utch)
from jess.tat, jess.tad
where tat.tag_id=tad.tag_id AND tat.utch=tad.utch
group by tat.tag_id
order by tat.tag_id;


--Consultar quantas datas possuem valores correspondentes de TAT e TAD
--cria tabela consulta1 com tds as datas onde ocorreram valores correspondentes dos 2 paramtros 
select tat.tag_id, tat.utch --into jess.consulta1
from jess.tat, jess.tad
where tat.tag_id=tad.tag_id AND tat.utch=tad.utch
group by tat.tag_id, tat.utch
order by tat.tag_id, tat.utch;

--Conta quantas correncias
select COUNT(*), consulta1.tag_id
from tags.consulta1
group by consulta1.tag_id
order by consulta1.tag_id

--MESMA CONSULTA PARA DDU E DDE
select dde.tag_id, dde.utch--, dde.b01, dde.b02, dde.b03, dde.b04, dde.b05, dde.b06, dde.b07, dde.b08, dde.b09, dde.b10, dde.b11, dde.b12, dde.b13, dde.b14--into jess.consulta2
from jess.dde, jess.ddu
where dde.tag_id=ddu.tag_id AND dde.utch=ddu.utch
--group by dde.tag_id, dde.utch
order by dde.tag_id, dde.utch;


------------------PAREI AQUI!!!----------------------------------
drop table if exists jess.ddeddu cascade;
create table jess.ddeddu as
select u.key_column as dduind, e.key_column as ddeind, u.nome_tarta,u.tag_id,u.utch,e.b01 as b01e,u.b01 as b01u, e.b02 as b02e,u.b02 as b02u, e.b03 as b03e,
u.b03 as b03u,e.b04 as b04e,u.b04 as b04u, e.b05 as b05e,u.b05 as b05u, e.b06 as b06e,u.b06 as b06u, e.b07 as b07e,u.b07 as b07u, e.b08 as b08e,u.b08 as b08u,
e.b09 as b09e,u.b09 as b09u, e.b10 as b10e,u.b10 as b10u,e.b11 as b11e, u.b11 as b11u, e.b12 as b12e,u.b12 as b12u, e.b13 as b13e,u.b13 as b13u, e.b14 as b14e,u.b14 as b14u
--,h.bin_02 as b02,h.bin_03 as b03,h.bin_04 as b04,h.bin_05 as b05,h.bin_06 as b06,
--h.bin_07 as b07,h.bin_08 as b08,h.bin_09 as b09,h.bin_10 as b10,h.bin_11 as b11,h.bin_12 as b12,h.bin_13 as b13,h.bin_14 as b14,t.sst,t.grad,t.distf,t.prof,t.sector,i.min/3600 as mutc --em horas
from jess.ddu u
join jess.dde e on (e.tag_id=u.tag_id and e.utch=u.utch)

-----Quantos dados distintos nos temos:
--n sei pq os passos acima estao retornando dados duplicados, ou temos dados duplicado e temos q arrumar, possivel!!
select utch,tag_id from jess.ddeddu
group by utch,tag_id




select COUNT(*), consulta2.tag_id
from tags.consulta2
group by consulta2.tag_id
order by consulta2.tag_id

select consulta1.tag_id, consulta1.utc into tags.consulta3
from tags.consulta1, tags.consulta2
where consulta1.tag_id=consulta2.tag_id AND consulta1.utc=consulta2.utc
group by consulta1.tag_id, consulta1.utc
order by consulta1.tag_id, consulta1.utc;

select COUNT(*), consulta3.tag_id
from tags.consulta3
group by consulta3.tag_id
order by consulta3.tag_id


SELECT TAT.tag_id, TAT.utc, 
       sum((TAT.tag_id=TAD.tag_id AND TAT.utc=TAD.utc)::int) as total
from tags.TAT, tags.TAD
group by TAT.tag_id, TAT.utc
order by TAT.tag_id, TAT.utc;

CREATE TABLE  AS SELECT * FROM tabealexistente;
--TAD
CREATE TABLE tags.TAD
SELECT *
FROM tags.histo_temp
WHERE hyst_type = TAD

--DDE
CREATE TABLE tags.DDE
SELECT *
FROM jess.histag
WHERE hyst_type = DiveDepth


--DDV
CREATE TABLE tags.DDU
SELECT *
FROM tags.histo_temp
WHERE hyst_type = DiveDuration


select count(*)
from tags.TAT, tags.TAD
where tag_id=140031 AND hyst_type='TAT'
--where tag_id=140031 AND hyst_type='TAD'
--where tag_id=140031 AND hyst_type='DiveDuration'
where tag_id=140031 AND hyst_type='DiveDepth'

--Soma da DDE em cada bin
SELECT sum(bin_01) as dde_10, sum(bin_02) as dde_20, sum(bin_03) as dde_50, sum(bin_04) as dde_100, sum(bin_05) as dde_150,
sum(bin_06) as dde_200, sum(bin_07) as dde_300, sum(bin_08) as dde_400, sum(bin_09) as dde_500, sum(bin_10) as dde_600,
sum(bin_11) as dde_700, sum(bin_12) as dde_maior_700, tag_id 
FROM tags.dde
where tag_id=140027 or tag_id=140029 or tag_id=140034 or tag_id=140037
group by tag_id

--Soma da DDU em cada bin
SELECT sum(bin_01) as ddu_10, sum(bin_02) as ddu_2, sum(bin_03) as ddu_3, sum(bin_04) as ddu_4, sum(bin_05) as ddu_5,
sum(bin_06) as ddu_10, sum(bin_07) as ddu_15, sum(bin_08) as ddu_20, sum(bin_09) as ddu_25, sum(bin_10) as ddu_30,
sum(bin_11) as ddu_40, sum(bin_12) as ddu_50, sum(bin_13) as ddu_60, sum(bin_14) as ddu_maior_60 ,tag_id 
FROM tags.ddu
where tag_id=140027 or tag_id=140029 or tag_id=140034 or tag_id=140037
group by tag_id

--Somo da TAT em cada bin
SELECT sum(bin_01) as tat_menos_2, sum(bin_02) as tata_0, sum(bin_03) as tat_3, sum(bin_04) as tat_6, sum(bin_05) as tat_9,
sum(bin_06) as tat_12, sum(bin_07) as tat_15, sum(bin_08) as tat_18, sum(bin_09) as tat_21, sum(bin_10) as tat_24,
sum(bin_11) as tat_27, sum(bin_12) as tat_30, sum(bin_13) as tat_33, sum(bin_14) as tat_maior_33 ,tag_id 
FROM tags.tat
where tag_id=140027 or tag_id=140029 or tag_id=140034 or tag_id=140037
group by tag_id

--Soma da TAD em cada bin
SELECT sum(bin_01) as tad_0, sum(bin_02) as tad_2, sum(bin_03) as tad_10, sum(bin_04) as tad_20, sum(bin_05) as tad_50,
sum(bin_06) as tad_100, sum(bin_07) as tad_150, sum(bin_08) as tad_200, sum(bin_09) as tad_300, sum(bin_10) as tad_400,
sum(bin_11) as tad_500, sum(bin_12) as tad_600, sum(bin_13) as tad_700, sum(bin_14) as tad_maior_700 ,tag_id 
FROM tags.tad
where tag_id=140027 or tag_id=140029 or tag_id=140034 or tag_id=140037
group by tag_id

--------------------------------------------------------------------------------------------------------------------------
--Configuração dos segundo lote:

--Soma da DDE em cada bin
SELECT sum(b01) as dde_05, sum(b02) as dde_10, sum(b03) as dde_15, sum(b04) as dde_20, sum(b05) as dde_25,
sum(b06) as dde_30, sum(b07) as dde_40, sum(b08) as dde_50, sum(b09) as dde_60, sum(b10) as dde_70,
sum(b11) as dde_100, sum(b12) as dde_150, sum(b13) as dde_200, sum(b14) as dde_maior_200, conf 
FROM jess.dde
group by conf

--Soma da TAD em cada bin
SELECT sum(b01) as dde_05, sum(b02) as dde_10, sum(b03) as dde_15, sum(b04) as dde_20, sum(b05) as dde_25,
sum(b06) as dde_30, sum(b07) as dde_40, sum(b08) as dde_50, sum(b09) as dde_60, sum(b10) as dde_70,
sum(b11) as dde_100, sum(b12) as dde_150, sum(b13) as dde_200, sum(b14) as dde_maior_200, conf
FROM jess.tad
group by conf

--Soma da TAT em cada bin
SELECT sum(b01) as dde_05, sum(b02) as dde_10, sum(b03) as dde_15, sum(b04) as dde_20, sum(b05) as dde_25,
sum(b06) as dde_30, sum(b07) as dde_40, sum(b08) as dde_50, sum(b09) as dde_60, sum(b10) as dde_70,
sum(b11) as dde_100, sum(b12) as dde_150, sum(b13) as dde_200, sum(b14) as dde_maior_200, conf
FROM jess.tat
group by conf

--Soma da DDU em cada bin
SELECT sum(b01) as dde_05, sum(b02) as dde_10, sum(b03) as dde_15, sum(b04) as dde_20, sum(b05) as dde_25,
sum(b06) as dde_30, sum(b07) as dde_40, sum(b08) as dde_50, sum(b09) as dde_60, sum(b10) as dde_70,
sum(b11) as dde_100, sum(b12) as dde_150, sum(b13) as dde_200, sum(b14) as dde_maior_200
FROM jess.ddu
