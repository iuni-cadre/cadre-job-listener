--Degree 1 query

--Get Degree 0
CALL apoc.export.csv.query('CALL apoc.load.jdbc(\'postgresql_url\', \"SELECT 
id, 
year::varchar, 
number::varchar,
issue::varchar, 
pages::varchar, 
authors_full_name::varchar, 
authors_id_orcid::varchar, 
authors_id_research::varchar, 
authors_prefix::varchar, 
authors_first_name::varchar, 
authors_middle_name::varchar, 
authors_last_name::varchar, 
authors_suffix::varchar, 
authors_initials::varchar, 
authors_display_name::varchar,
authors_wos_name::varchar, 
authors_id_lang::varchar, 
authors_email::varchar, 
reference::varchar, 
issn::varchar, 
doi::varchar, 
title::varchar, 
journal_name::varchar, 
journal_abbrev::varchar, 
journal_iso::varchar, 
abstract_paragraph::varchar
FROM wos_core.interface_table
WHERE title_tsv @@ to_tsquery(\'Fish\')
LIMIT 1000
") YIELD row
RETURN row.id as wos_id, 
	   row.year as year, 
	   row.number as issue, 
	   row.pages as pages, 
	   row.authors_full_name as authors_full_name, 
	   row.authors_id_orcid as authors_id_orcid, 
	   row.authors_id_dais as authors_id_dais, 
	   row.authors_id_research as authors_id_research, 
	   row.authors_prefix as authors_prefix, 
	   row.authors_first_name as authors_first_name, 
	   row.authors_middle_name as authors_middle_name, 
	   row.authors_last_name as authors_last_name, 
	   row.authors_suffix as authors_suffix, 
	   row.authors_initials as authors_initials, 
	   row.authors_display_name as authors_display_name, 
	   row.authors_wos_name as authors_wos_name, 
	   row.authors_id_lang as authors_id_lang, 
	   row.authors_email as authors_email, 
	   row.reference as reference, 
	   row.issn as issn, 
	   row.doi as doi, 
	   row.title as title, 
	   row.journal_name as journal_name,
	   row.journal_abbrev as journal_abbrev, 
	   row.journal_iso as journal_iso, 
	   row.abstract_paragraph as abstract_paragraph',
'degree0.csv', {})
;

--Get Edge List
CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///degree0.csv\' as pg_pap
MATCH (n:paper{paper_id:pg_pap.`wos_id`})<-[:REFERENCES]-(m:paper)
RETURN n.paper_id AS From , m.paper_id AS To', 'edge.csv', {})
;

--Get Node List
CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///edge.csv\' as edge
MATCH (n:paper)
WHERE n.paper_id IN [edge.`From`, edge.`To`]
RETURN DISTINCT(n.paper_id)       	 	 AS paper_id,
				n.pubyear AS pubyear, 
				n.issue AS issue, 
				n.issn AS issn, 
				n.has_abstract AS has_abstract, 
				n.authors_full_name AS authors_full_name, 
				n.journal_name AS journal_name, 
				n.pubtype AS pubtype, 
				n.title AS title, 
				n.vol AS vol
', 'node.csv', {}); 

-------------------------------------------------------------------------------------------------------------------------------------

--Degree 2 query


--Get Degree 0
CALL apoc.export.csv.query('CALL apoc.load.jdbc(\'postgresql_url\', \"SELECT 
id, 
year::varchar, 
number::varchar,
issue::varchar, 
pages::varchar, 
authors_full_name::varchar, 
authors_id_orcid::varchar, 
authors_id_research::varchar, 
authors_prefix::varchar, 
authors_first_name::varchar, 
authors_middle_name::varchar, 
authors_last_name::varchar, 
authors_suffix::varchar, 
authors_initials::varchar, 
authors_display_name::varchar,
authors_wos_name::varchar, 
authors_id_lang::varchar, 
authors_email::varchar, 
reference::varchar, 
issn::varchar, 
doi::varchar, 
title::varchar, 
journal_name::varchar, 
journal_abbrev::varchar, 
journal_iso::varchar, 
abstract_paragraph::varchar
FROM wos_core.interface_table
WHERE title_tsv @@ to_tsquery(\'Fish\')
LIMIT 1000") YIELD row
RETURN row.id as wos_id, 
	   row.year as year, 
	   row.number as issue, 
	   row.pages as pages, 
	   row.authors_full_name as authors_full_name, 
	   row.authors_id_orcid as authors_id_orcid, 
	   row.authors_id_dais as authors_id_dais, 
	   row.authors_id_research as authors_id_research, 
	   row.authors_prefix as authors_prefix, 
	   row.authors_first_name as authors_first_name, 
	   row.authors_middle_name as authors_middle_name, 
	   row.authors_last_name as authors_last_name, 
	   row.authors_suffix as authors_suffix, 
	   row.authors_initials as authors_initials, 
	   row.authors_display_name as authors_display_name, 
	   row.authors_wos_name as authors_wos_name, 
	   row.authors_id_lang as authors_id_lang, 
	   row.authors_email as authors_email, 
	   row.reference as reference, 
	   row.issn as issn, 
	   row.doi as doi, 
	   row.title as title, 
	   row.journal_name as journal_name,
	   row.journal_abbrev as journal_abbrev, 
	   row.journal_iso as journal_iso, 
	   row.abstract_paragraph as abstract_paragraph',
'degree0.csv', {})
;

--Get Edge List
CALL apoc.export.csv.query(
'LOAD CSV WITH HEADERS FROM \'file:///degree0.csv\' as pg_pap
MATCH (n:paper{paper_id:pg_pap.`wos_id`})<-[:REFERENCES]-(m:paper)
WITH
  COLLECT({from: n.paper_id, to: m.paper_id}) AS data1,
  [(m)<-[:REFERENCES]-(o:paper) | {from: m.paper_id, to: o.paper_id}] AS data2
UNWIND (data1 + data2) AS data
RETURN data.from AS From, data.to AS To', 'edge.csv', {})
;


--Get Node List
CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///edge.csv\' as edge
MATCH (n:paper)
WHERE n.paper_id IN [edge.`From`, edge.`To`]
RETURN DISTINCT(n.paper_id)       	 	 AS paper_id,
				n.pubyear AS pubyear, 
				n.issue AS issue, 
				n.issn AS issn, 
				n.has_abstract AS has_abstract, 
				n.authors_full_name AS authors_full_name, 
				n.journal_name AS journal_name, 
				n.pubtype AS pubtype, 
				n.title AS title, 
				n.vol AS vol
', 'node.csv', {}); 
