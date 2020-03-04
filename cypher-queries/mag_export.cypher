--Degree 1 query

--Get Degree 0
CALL apoc.export.csv.query('CALL apoc.load.jdbc(\'postgresql_url\', \"SELECT 
paper_id, 
author_id,	 
author_sequence_number::varchar,
authors_display_name,
authors_last_known_affiliation_id,
journal_id, 
conference_series_id,
conference_instance_id,
paper_reference_id,
field_of_study_id,
doi,
doc_type, 
paper_title,
original_title, 
book_title,
year,
date::varchar, 
paper_publisher,
issue, 
paper_abstract, 
paper_first_page, 
paper_last_page, 
paper_reference_count::varchar,
paper_citation_count::varchar, 
paper_estimated_citation::varchar,         
conference_display_name,          
journal_display_name,              
journal_issn,                      
journal_publisher
FROM mag_core.final_mag_interface_table
WHERE paper_title_tsv @@ to_tsquery(\'cattle\') 
LIMIT 1000") YIELD row 
RETURN row.paper_id               			 AS paper_id, 
       row.author_id                         AS author_id,	 
	   row.author_sequence_number            AS author_sequence_number,
       row.authors_display_name              AS authors_display_name,
       row.authors_last_known_affiliation_id AS authors_last_known_affiliation_id,
       row.journal                           AS journal_id, 
       row.conference_series_id              AS conference_series_id,
       row.conference_instance_id            AS conference_instance_id,
       row.paper_reference_id                AS paper_reference_id,
       row.field_of_study_id                 AS field_of_study_id,
       row.doi								 AS doi,
       row.doc_type                          AS doc_type, 
       row.paper_title                       AS paper_title,
       row.original_title                    AS original_title, 
       row.book_title                        AS book_title,
	   row.year 							 AS year,
       row.date                              AS date, 
	   row.paper_publisher                   AS paper_publisher,
       row.issue                             AS issue, 
       row.paper_abstract                    AS paper_abstract, 
       row.paper_first_page                  AS paper_first_page, 
 	   row.paper_last_page 				     AS paper_last_page, 
	   row.paper_reference_count             AS paper_reference_count,
       row.paper_citation_count              AS paper_citation_count, 
	   row.paper_estimated_citation          AS paper_estimated_citation, 
       row.conference_display_name           AS conference_display_name, 
       row.journal_display_name              AS journal_display_name, 
       row.journal_issn                      AS journal_issn, 
       row.journal_publisher                 AS journal_publisher',
'degree0.csv', {d: ';', quotes: false, format: 'plain'})
;
 


--Get Edge list
CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///degree0.csv\' as pg_pap
MATCH (n:paper{paper_id:pg_pap.`paper_id`})<-[:REFERENCES]-(m:paper)
RETURN n.paper_id AS From , m.paper_id AS To', 'edge.csv', {})
;


--Get Node List
CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///edge.csv\' as edge
MATCH (n:paper)
WHERE n.paper_id = edge.`From` OR n.paper_id = edge.`To`
RETURN DISTINCT(n.paper_id)       	 	 AS paper_id,
				n.date           	 	 AS date, 
                n.journal_id     	 	 AS journal_id, 
                n.citation_count 	 	 AS citation_count, 
                n.original_title 	 	 AS original_title, 
                n.issue          	 	 AS issue, 
                n.paper_title    	 	 AS paper_title, 
                n.year           	  	 AS year, 
                n.first_name             AS first_name,
                n.last_name      	 	 AS last_name, 
			    n.original_venue 	 	 AS original_venue, 
                n.doc_type       	 	 AS doc_type,
                n.volume             	 AS volume, 
                n.estimated_citation 	 AS estimated_citation,
				n.conference_instance_id AS conference_instance_id, 
                n.book_title             AS book_title, 
                n.rank                   AS rank, 
                n.publisher              AS publisher, 
                n.created_date           AS created_date, 
                n.reference_count        AS reference_count, 
                n.conference_series_id   AS conference_series_id, 
                n.doi                    AS doi
', 'node.csv', {}); 
 
-------------------------------------------------------------------------------------------------------------------------------------

--Degree 2 query


--Get Degree 0
CALL apoc.export.csv.query('CALL apoc.load.jdbc(\'postgresql_url\', \"SELECT 
paper_id, 
author_id,	 
author_sequence_number::varchar,
authors_display_name,
authors_last_known_affiliation_id,
journal_id, 
conference_series_id,
conference_instance_id,
paper_reference_id,
field_of_study_id,
doi,
doc_type, 
paper_title,
original_title, 
book_title,
year,
date::varchar, 
paper_publisher,
issue, 
paper_abstract, 
paper_first_page, 
paper_last_page, 
paper_reference_count::varchar,
paper_citation_count::varchar, 
paper_estimated_citation::varchar,         
conference_display_name,          
journal_display_name,              
journal_issn,                      
journal_publisher
FROM mag_core.final_mag_interface_table
WHERE paper_title_tsv @@ to_tsquery(\'duck\') 
LIMIT 1000") YIELD row 
RETURN row.paper_id               			 AS paper_id, 
       row.author_id                         AS author_id,	 
	   row.author_sequence_number            AS author_sequence_number,
       row.authors_display_name              AS authors_display_name,
       row.authors_last_known_affiliation_id AS authors_last_known_affiliation_id,
       row.journal                           AS journal_id, 
       row.conference_series_id              AS conference_series_id,
       row.conference_instance_id            AS conference_instance_id,
       row.paper_reference_id                AS paper_reference_id,
       row.field_of_study_id                 AS field_of_study_id,
       row.doi								 AS doi,
       row.doc_type                          AS doc_type, 
       row.paper_title                       AS paper_title,
       row.original_title                    AS original_title, 
       row.book_title                        AS book_title,
	   row.year 							 AS year,
       row.date                              AS date, 
	   row.paper_publisher                   AS paper_publisher,
       row.issue                             AS issue, 
       row.paper_abstract                    AS paper_abstract, 
       row.paper_first_page                  AS paper_first_page, 
 	   row.paper_last_page 				     AS paper_last_page, 
	   row.paper_reference_count             AS paper_reference_count,
       row.paper_citation_count              AS paper_citation_count, 
	   row.paper_estimated_citation          AS paper_estimated_citation, 
       row.conference_display_name           AS conference_display_name, 
       row.journal_display_name              AS journal_display_name, 
       row.journal_issn                      AS journal_issn, 
       row.journal_publisher                 AS journal_publisher',
'degree0.csv', { format: 'plain'})
;
 


--Get Edge list
CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///degree0.csv\' as pg_pap
MATCH (n:paper{paper_id:pg_pap.`paper_id`})<-[:REFERENCES]-(m:paper)
RETURN n.paper_id AS From , m.paper_id AS To
UNION ALL
LOAD CSV WITH HEADERS FROM \'file:///degree0.csv\' as pg_pap
MATCH (n:paper{paper_id:pg_pap.`paper_id`})<-[:REFERENCES]-(m:paper)<-[:REFERENCES]-(o:paper)
RETURN m.paper_id AS From, o.paper_id AS To
', 'edge.csv', {})
;




CALL apoc.export.csv.query(
'LOAD CSV WITH HEADERS FROM \'file:///degree0.csv\' as pg_pap
MATCH (n:paper{paper_id:pg_pap.`paper_id`})<-[:REFERENCES]-(m:paper)
WITH
  COLLECT({from: n.paper_id, to: m.paper_id}) AS data1,
  [(m)<-[:REFERENCES]-(o:paper) | {from: m.paper_id, to: o.paper_id}] AS data2
UNWIND (data1 + data2) AS data
RETURN data.from AS From, data.to AS To', 'edge.csv', {})
;



--Get Node list

CALL apoc.export.csv.query('LOAD CSV WITH HEADERS FROM \'file:///edge.csv\' as edge
MATCH (n:paper)
WHERE n.paper_id IN [edge.`From`, edge.`To`]
RETURN DISTINCT(n.paper_id)       	 	 AS paper_id,
				n.date           	 	 AS date, 
                n.journal_id     	 	 AS journal_id, 
                n.citation_count 	 	 AS citation_count, 
                n.original_title 	 	 AS original_title, 
                n.issue          	 	 AS issue, 
                n.paper_title    	 	 AS paper_title, 
                n.year           	  	 AS year, 
                n.first_name             AS first_name,
                n.last_name      	 	 AS last_name, 
			    n.original_venue 	 	 AS original_venue, 
                n.doc_type       	 	 AS doc_type,
                n.volume             	 AS volume, 
                n.estimated_citation 	 AS estimated_citation,
				n.conference_instance_id AS conference_instance_id, 
                n.book_title             AS book_title, 
                n.rank                   AS rank, 
                n.publisher              AS publisher, 
                n.created_date           AS created_date, 
                n.reference_count        AS reference_count, 
                n.conference_series_id   AS conference_series_id, 
                n.doi                    AS doi
', 'node.csv', {}); 
 

--reboot for ulimit
--reformat /data to ext4
--replace efs with FSx
--- sudo echo 'deadline' > /sys/block/nvme0n1/queue/scheduler -AWS doesn't allow
-- Cluste mag_interface table on paper_id index -https://www.postgresql.org/docs/11/sql-cluster.html



