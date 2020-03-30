import unittest
import backend.job_listener
from unittest.mock import patch, MagicMock, mock_open, call

class MyTestCase(unittest.TestCase):

    @patch('backend.job_listener.util')
    @patch('backend.job_listener.boto3')
    def test_something(self, boto, util):
        pass
        #backend.job_listener.poll_queue()
        #self.assertEqual(True, False)

    @patch('backend.job_listener.logger')
    def test_get_file_name(self, mock_logger):
        self.assertEqual('jobname_1234', backend.job_listener.get_file_name('1234', 'jobname'))
        self.assertEqual('1234', backend.job_listener.get_file_name('1234', '1234'))
        self.assertEqual('1234', backend.job_listener.get_file_name('1234', ''))

    @patch('backend.job_listener.logger')
    def test_generate_csv_fields_neo4j_wos(self, logger):
        self.assertEqual("", backend.job_listener.generate_csv_fields_neo4j_wos([]))
        self.assertEqual("row.id as wos_id,row.issn as issn", backend.job_listener.generate_csv_fields_neo4j_wos(["id", "issn"]))

    @patch('backend.job_listener.logger')
    def test_generate_output_string_neo4j_wos(self, logger):
        self.assertEqual("", backend.job_listener.generate_output_string_neo4j_wos([]))
        self.assertEqual("id,issn::varchar", backend.job_listener.generate_output_string_neo4j_wos(["id", "issn"]))
        self.assertEqual("number::varchar,issn::varchar",
                         backend.job_listener.generate_output_string_neo4j_wos(["number", "issn"]))

    @patch('backend.job_listener.logger')
    def test_generate_csv_fields_neo4j_mag(self, logger):
        self.assertEqual("", backend.job_listener.generate_csv_fields_neo4j_mag([]))
        self.assertEqual("row.paper_id AS paper_id,row.journal_issn AS journal_issn",
                         backend.job_listener.generate_csv_fields_neo4j_mag(["paper_id", "journal_issn"]))

    @patch('backend.job_listener.logger')
    def test_generate_output_string_neo4j_mag(self, logger):
        self.assertEqual("", backend.job_listener.generate_output_string_neo4j_mag([]))
        self.assertEqual("paper_id,journal_issn",
                         backend.job_listener.generate_output_string_neo4j_mag(["paper_id", "journal_issn"]))

    @patch('backend.job_listener.logger')
    def test_get_node_list_wos(self, logger):
        actual = backend.job_listener.get_node_list_wos("edgefile", "nodefile")
        self.assertIn("LOAD CSV WITH HEADERS FROM \\'file:///edgefile\\' as edge", actual)
        self.assertIn( "n.vol AS vol','nodefile'", actual)


    @patch('backend.job_listener.logger')
    def test_get_edge_list_degree_2_wos(self, logger):
        actual = backend.job_listener.get_edge_list_degree_2_wos("csvfile", "edgefile")
        self.assertIn("LOAD CSV WITH HEADERS FROM \\'file:///csvfile\\' AS pg_pap", actual)
        self.assertIn("WITH COLLECT ({from:n.paper_id, to: m.paper_id}) AS data1", actual)
        self.assertIn("RETURN data.from AS Citing, data.to AS Cited','edgefile',{})", actual)

    @patch('backend.job_listener.logger')
    def test_get_edge_list_degree_1_wos(self, logger):
        actual = backend.job_listener.get_edge_list_degree_1_wos("csvfile", "edgefile")
        self.assertIn("LOAD CSV WITH HEADERS FROM \\'file:///csvfile\\' AS pg_pap", actual)
        self.assertIn("RETURN n.paper_id AS Citing , m.paper_id AS Cited','edgefile', {})", actual)


    @patch('backend.job_listener.logger')
    def test_get_node_list_mag(self, logger):
        actual = backend.job_listener.get_node_list_mag("edgefile", "nodefile")
        self.assertIn("LOAD CSV WITH HEADERS FROM \\'file:///edgefile\\' as edge MATCH(n:paper)", actual)
        self.assertIn("RETURN DISTINCT(n.paper_id) AS paper_id,", actual)
        self.assertIn("n.doi AS doi','nodefile', {})", actual)

    @patch('backend.job_listener.logger')
    def test_degree_0_query(self, logger):
        actual = backend.job_listener.degree_0_query("intfquery", "filename", "fieldnames")
        self.assertIn("CALL apoc.load.jdbc(\\\'postgresql_url\\\', \"intfquery\")", actual)
        self.assertIn("RETURN fieldnames\', \'filename\',{format: \'plain\'})", actual)

    @patch('backend.job_listener.logger')
    def test_convert_csv_to_json(self, logger):
        with patch("builtins.open", mock_open(read_data='d,e,f\na,b,c\n')) as mock_file:
            backend.job_listener.convert_csv_to_json("csvpath", "jsonpath", "field1,field2,field3")
            mock_file.assert_has_calls([call('csvpath', 'r'), call('jsonpath', 'w')])
            handle = mock_file()
            actual = "".join(x[0][0] for x in handle.write.call_args_list)
            expected = '{"field1": "d", "field2": "e", "field3": "f"}\n{"field1": "a", "field2": "b", "field3": "c"}\n'
            self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()
