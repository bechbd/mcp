# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the NeptuneDatabase class."""

import json
import pytest
from awslabs.amazon_neptune_mcp_server.exceptions import NeptuneException
from awslabs.amazon_neptune_mcp_server.graph_store.database import NeptuneDatabase
from awslabs.amazon_neptune_mcp_server.models import GraphSchema, RDFGraphSchema
from unittest.mock import MagicMock, patch


@pytest.mark.asyncio
class TestNeptuneDatabase:
    """Test class for the NeptuneDatabase functionality."""

    @patch('boto3.Session')
    async def test_init_success(self, mock_session):
        """Test successful initialization of NeptuneDatabase.
        This test verifies that:
        1. The boto3 Session is created correctly
        2. The client is created with the correct parameters
        3. The schema is refreshed during initialization.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock API responses
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {'graphSummary': {'classes': [], 'predicates': []}}
        }

        # Mock _refresh_lpg_schema to avoid actual API calls
        with (
            patch.object(
                NeptuneDatabase,
                '_refresh_lpg_schema',
                return_value=GraphSchema(nodes=[], relationships=[], relationship_patterns=[]),
            ),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Act
            db = NeptuneDatabase(host='test-endpoint', port=8182, use_https=True)

            # Assert
            mock_session.assert_called_once()
            mock_session_instance.client.assert_called_once_with(
                'neptunedata', endpoint_url='https://test-endpoint:8182'
            )
            assert db.client == mock_client

    @patch('boto3.Session')
    async def test_init_with_credentials_profile(self, mock_session):
        """Test initialization with a credentials profile.
        This test verifies that:
        1. The boto3 Session is created with the specified profile name
        2. The client is created with the correct parameters.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock API responses
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {'graphSummary': {'classes': [], 'predicates': []}}
        }

        # Mock _refresh_lpg_schema to avoid actual API calls
        with (
            patch.object(
                NeptuneDatabase,
                '_refresh_lpg_schema',
                return_value=GraphSchema(nodes=[], relationships=[], relationship_patterns=[]),
            ),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Act
            NeptuneDatabase(
                host='test-endpoint',
                port=8182,
                use_https=True,
                credentials_profile_name='test-profile',
            )

            # Assert
            mock_session.assert_called_once_with(profile_name='test-profile')
            mock_session_instance.client.assert_called_once_with(
                'neptunedata', endpoint_url='https://test-endpoint:8182'
            )

    @patch('boto3.Session')
    async def test_init_with_http(self, mock_session):
        """Test initialization with HTTP instead of HTTPS.
        This test verifies that:
        1. The client is created with an HTTP endpoint URL when use_https is False.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock API responses
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': {'nodeLabels': [], 'edgeLabels': []}}
        }
        mock_client.get_rdf_graph_summary.return_value = {
            'payload': {'graphSummary': {'classes': [], 'predicates': []}}
        }

        # Mock _refresh_lpg_schema to avoid actual API calls
        with (
            patch.object(
                NeptuneDatabase,
                '_refresh_lpg_schema',
                return_value=GraphSchema(nodes=[], relationships=[], relationship_patterns=[]),
            ),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Act
            NeptuneDatabase(host='test-endpoint', port=8182, use_https=False)

            # Assert
            mock_session_instance.client.assert_called_once_with(
                'neptunedata', endpoint_url='http://test-endpoint:8182'
            )

    @patch('boto3.Session')
    async def test_init_session_error(self, mock_session):
        """Test handling of session creation errors.
        This test verifies that:
        1. Errors during session creation are properly caught and re-raised
        2. The error message is appropriate.
        """
        # Arrange
        mock_session.side_effect = Exception('Auth error')

        # Act & Assert
        with pytest.raises(
            ValueError, match='Could not load credentials to authenticate with AWS client'
        ):
            NeptuneDatabase(host='test-endpoint')


    @patch('boto3.Session')
    async def test_get_summary_success(self, mock_session):
        """Test successful retrieval of graph summary.
        This test verifies that:
        1. The get_propertygraph_summary API is called
        2. The summary data is correctly extracted from the response.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API response
        mock_summary = {'nodeLabels': ['Person', 'Movie'], 'edgeLabels': ['ACTED_IN', 'DIRECTED']}
        mock_client.get_propertygraph_summary.return_value = {
            'payload': {'graphSummary': mock_summary}
        }

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            result = db._get_summary()

            # Assert
            mock_client.get_propertygraph_summary.assert_called_once()
            assert result == mock_summary

    @patch('boto3.Session')
    async def test_get_summary_api_error(self, mock_session):
        """Test handling of API errors in get_summary.
        This test verifies that:
        1. API errors are properly caught and re-raised as NeptuneException
        2. The error message indicates the Summary API is not available.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API to raise an exception
        mock_client.get_propertygraph_summary.side_effect = Exception('API error')

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act & Assert
            with pytest.raises(NeptuneException) as exc_info:
                db._get_summary()

            # Check the exception details
            assert 'Summary API is not available' in exc_info.value.message
            assert 'API error' in exc_info.value.details

    @patch('boto3.Session')
    async def test_get_summary_invalid_response(self, mock_session):
        """Test handling of invalid responses in get_summary.
        This test verifies that:
        1. Invalid responses are properly caught and re-raised as NeptuneException
        2. The error message indicates the response was invalid.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API to return an invalid response
        class MockResponse:
            def __init__(self):
                self.payload = {}  # Missing graphSummary
                self.content = b'Invalid response'

            def __getitem__(self, key):
                return getattr(self, key)

        mock_client.get_propertygraph_summary.return_value = MockResponse()

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act & Assert
            with pytest.raises(NeptuneException) as exc_info:
                db._get_summary()

            # Check the exception details
            assert 'Summary API did not return a valid response' in exc_info.value.message

    @patch('boto3.Session')
    async def test_get_labels(self, mock_session):
        """Test retrieval of node and edge labels.
        This test verifies that:
        1. The _get_summary method is called
        2. Node and edge labels are correctly extracted from the summary.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Mock _get_summary
            mock_summary = {
                'nodeLabels': ['Person', 'Movie'],
                'edgeLabels': ['ACTED_IN', 'DIRECTED'],
            }
            with patch.object(db, '_get_summary', return_value=mock_summary):
                # Act
                n_labels, e_labels = db._get_labels()

                # Assert
                assert n_labels == ['Person', 'Movie']
                assert e_labels == ['ACTED_IN', 'DIRECTED']

    @patch('boto3.Session')
    async def test_query_opencypher_without_params(self, mock_session):
        """Test execution of openCypher queries without parameters.
        This test verifies that:
        1. The execute_open_cypher_query API is called with the correct query
        2. The result is correctly extracted from the response.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API response
        mock_result = [{'n': {'id': '1'}}]
        mock_client.execute_open_cypher_query.return_value = {'result': mock_result}

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            result = db.query_opencypher('MATCH (n) RETURN n LIMIT 1')

            # Assert
            mock_client.execute_open_cypher_query.assert_called_once_with(
                openCypherQuery='MATCH (n) RETURN n LIMIT 1'
            )
            assert result == mock_result

    @patch('boto3.Session')
    async def test_query_opencypher_with_params(self, mock_session):
        """Test execution of openCypher queries with parameters.
        This test verifies that:
        1. The execute_open_cypher_query API is called with the correct query and parameters
        2. The parameters are properly JSON-encoded
        3. The result is correctly extracted from the response.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API response
        mock_result = [{'n': {'id': '1'}}]
        mock_client.execute_open_cypher_query.return_value = {'result': mock_result}

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            params = {'id': '1'}
            result = db.query_opencypher('MATCH (n) WHERE n.id = $id RETURN n', params)

            # Assert
            mock_client.execute_open_cypher_query.assert_called_once_with(
                openCypherQuery='MATCH (n) WHERE n.id = $id RETURN n',
                parameters=json.dumps(params),
            )
            assert result == mock_result

    @patch('boto3.Session')
    async def test_query_opencypher_results_format(self, mock_session):
        """Test handling of different result formats in openCypher queries.
        This test verifies that:
        1. The method correctly handles responses with 'results' instead of 'result'.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API response with 'results' instead of 'result'
        mock_results = [{'n': {'id': '1'}}]
        mock_client.execute_open_cypher_query.return_value = {'results': mock_results}

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            result = db.query_opencypher('MATCH (n) RETURN n LIMIT 1')

            # Assert
            assert result == mock_results

    @patch('boto3.Session')
    async def test_query_gremlin(self, mock_session):
        """Test execution of Gremlin queries.
        This test verifies that:
        1. The execute_gremlin_query API is called with the correct query
        2. The serializer parameter is correctly set
        3. The result is correctly extracted from the response.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API response
        mock_result = [{'id': '1'}]
        mock_client.execute_gremlin_query.return_value = {'result': mock_result}

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            result = db.query_gremlin('g.V().limit(1)')

            # Assert
            mock_client.execute_gremlin_query.assert_called_once()
            assert result == mock_result

    @patch('boto3.Session')
    async def test_query_gremlin_results_format(self, mock_session):
        """Test handling of different result formats in Gremlin queries.
        This test verifies that:
        1. The method correctly handles responses with 'results' instead of 'result'.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock the API response with 'results' instead of 'result'
        mock_results = [{'id': '1'}]
        mock_client.execute_gremlin_query.return_value = {'results': mock_results}

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema'),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            result = db.query_gremlin('g.V().limit(1)')

            # Assert
            assert result == mock_results

    @patch('boto3.Session')
    async def test_get_schema_cached(self, mock_session):
        """Test that get_schema returns cached schema when available.
        This test verifies that:
        1. When schema is already cached, _refresh_lpg_schema is not called
        2. The cached schema is returned.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Create a mock schema
        mock_schema = GraphSchema(nodes=[], relationships=[], relationship_patterns=[])

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema', return_value=mock_schema),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Act
            result = db.get_lpg_schema()

            # Assert - just verify the result is the mock schema
            assert result == mock_schema
            assert result == mock_schema

    @patch('boto3.Session')
    async def test_get_schema_refresh(self, mock_session):
        """Test that get_schema refreshes schema when not cached.
        This test verifies that:
        1. When schema is not cached, _refresh_lpg_schema is called
        2. The refreshed schema is returned.
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'  # Add region_name attribute
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Create a mock schema
        mock_schema = GraphSchema(nodes=[], relationships=[], relationship_patterns=[])

        # Mock _refresh_lpg_schema to avoid actual API calls during init
        with (
            patch.object(NeptuneDatabase, '_refresh_lpg_schema', return_value=mock_schema),
            patch.object(
                NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}
            ),
        ):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')

            # Set schema to None to force refresh
            db.schema = None

            # Reset the mock to verify it's called again
            NeptuneDatabase._refresh_lpg_schema.reset_mock()
            NeptuneDatabase._refresh_lpg_schema.return_value = mock_schema

            # Act
            result = db.get_lpg_schema()

            # Assert
            NeptuneDatabase._refresh_lpg_schema.assert_called_once()
            assert result == mock_schema

    @patch('boto3.Session')
    async def test_get_triples(self, mock_session):
        """Test retrieval of relationship patterns (triples).
        
        This test verifies that:
        1. The query_opencypher method is called for each edge label
        2. The relationship patterns are correctly created from the query results
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Mock query_opencypher to return test data
            db.query_opencypher = MagicMock()
            db.query_opencypher.side_effect = [
                [
                    {'from': ['Person'], 'edge': 'KNOWS', 'to': ['Person']},
                    {'from': ['Person'], 'edge': 'KNOWS', 'to': ['Person']}
                ],
                [
                    {'from': ['Person'], 'edge': 'ACTED_IN', 'to': ['Movie']},
                    {'from': ['Director'], 'edge': 'ACTED_IN', 'to': ['Movie']}
                ]
            ]
            
            # Act
            result = db._get_triples(['KNOWS', 'ACTED_IN'])
            
            # Assert
            assert len(result) == 4
            assert db.query_opencypher.call_count == 2
            
            # Check the first relationship pattern
            assert result[0].left_node == 'Person'
            assert result[0].relation == 'KNOWS'
            assert result[0].right_node == 'Person'
            
            # Check the third relationship pattern
            assert result[2].left_node == 'Person'
            assert result[2].relation == 'ACTED_IN'
            assert result[2].right_node == 'Movie'

    @patch('boto3.Session')
    async def test_get_node_properties(self, mock_session):
        """Test retrieval of node properties.
        
        This test verifies that:
        1. The query_opencypher method is called for each node label
        2. The node properties are correctly extracted from the query results
        3. The property types are correctly mapped
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Mock query_opencypher to return test data
            db.query_opencypher = MagicMock()
            db.query_opencypher.side_effect = [
                [
                    {'props': {'name': 'John', 'age': 30, 'active': True}},
                    {'props': {'name': 'Jane', 'age': 25, 'score': 4.5}}
                ],
                [
                    {'props': {'title': 'The Matrix', 'year': 1999}},
                    {'props': {'title': 'Inception', 'year': 2010}}
                ]
            ]
            
            # Define type mapping
            types = {
                'str': 'STRING',
                'int': 'INTEGER',
                'float': 'DOUBLE',
                'bool': 'BOOLEAN'
            }
            
            # Act
            result = db._get_node_properties(['Person', 'Movie'], types)
            
            # Assert
            assert len(result) == 2
            assert db.query_opencypher.call_count == 2
            
            # Check the Person node
            person_node = result[0]
            assert person_node.labels == 'Person'
            assert len(person_node.properties) == 4
            
            # Check property types
            prop_types = {p.name: p.type for p in person_node.properties}
            assert 'STRING' in prop_types['name']
            assert 'INTEGER' in prop_types['age']
            assert 'BOOLEAN' in prop_types['active']
            assert 'DOUBLE' in prop_types['score']
            
            # Check the Movie node
            movie_node = result[1]
            assert movie_node.labels == 'Movie'
            assert len(movie_node.properties) == 2
            
            # Check property types
            prop_types = {p.name: p.type for p in movie_node.properties}
            assert 'STRING' in prop_types['title']
            assert 'INTEGER' in prop_types['year']

    @patch('boto3.Session')
    async def test_get_edge_properties(self, mock_session):
        """Test retrieval of edge properties.
        
        This test verifies that:
        1. The query_opencypher method is called for each edge label
        2. The edge properties are correctly extracted from the query results
        3. The property types are correctly mapped
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Mock query_opencypher to return test data
            db.query_opencypher = MagicMock()
            db.query_opencypher.side_effect = [
                [
                    {'props': {'since': '2020-01-01', 'strength': 0.8}},
                    {'props': {'since': '2019-05-15', 'strength': 0.6}}
                ],
                [
                    {'props': {'role': 'Neo', 'screenTime': 120}},
                    {'props': {'role': 'Trinity', 'screenTime': 90}}
                ]
            ]
            
            # Define type mapping
            types = {
                'str': 'STRING',
                'int': 'INTEGER',
                'float': 'DOUBLE'
            }
            
            # Act
            result = db._get_edge_properties(['KNOWS', 'ACTED_IN'], types)
            
            # Assert
            assert len(result) == 2
            assert db.query_opencypher.call_count == 2
            
            # Check the KNOWS relationship
            knows_rel = result[0]
            assert knows_rel.type == 'KNOWS'
            assert len(knows_rel.properties) == 2
            
            # Check property types
            prop_types = {p.name: p.type for p in knows_rel.properties}
            assert 'STRING' in prop_types['since']
            assert 'DOUBLE' in prop_types['strength']
            
            # Check the ACTED_IN relationship
            acted_in_rel = result[1]
            assert acted_in_rel.type == 'ACTED_IN'
            assert len(acted_in_rel.properties) == 2
            
            # Check property types
            prop_types = {p.name: p.type for p in acted_in_rel.properties}
            assert 'STRING' in prop_types['role']
            assert 'INTEGER' in prop_types['screenTime']


    @patch('boto3.Session')
    async def test_propertygraph_schema(self, mock_session):
        """Test that propertygraph_schema calls get_lpg_schema.
        
        This test verifies that:
        1. The propertygraph_schema method calls get_lpg_schema
        2. The result from get_lpg_schema is returned unchanged
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Create a mock schema
            mock_schema = GraphSchema(nodes=[], relationships=[], relationship_patterns=[])
            
            # Mock get_lpg_schema
            db.get_lpg_schema = MagicMock(return_value=mock_schema)
            
            # Act
            result = db.propertygraph_schema()
            
            # Assert
            db.get_lpg_schema.assert_called_once()
            assert result == mock_schema

    @patch('boto3.Session')
    async def test_query_sparql(self, mock_session):
        """Test execution of SPARQL queries.
        
        This test verifies that:
        1. The _query_sparql method is called with the correct query
        2. The result is returned unchanged
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Mock _query_sparql
            mock_result = {'results': {'bindings': [{'s': {'value': 'http://example.org/subject'}}]}}
            db._query_sparql = MagicMock(return_value=mock_result)
            
            # Act
            query = 'SELECT * WHERE { ?s ?p ?o } LIMIT 1'
            result = db.query_sparql(query)
            
            # Assert
            db._query_sparql.assert_called_once_with(query)
            assert result == mock_result

    @patch('boto3.Session')
    async def test_get_local_name_with_hash(self, mock_session):
        """Test extraction of local name from IRI with hash.
        
        This test verifies that:
        1. The _get_local_name method correctly splits IRIs with hash
        2. The prefix and local name are correctly returned
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Act
            iri = 'http://example.org/ontology#Person'
            prefix, local = db._get_local_name(iri)
            
            # Assert
            assert prefix == 'http://example.org/ontology#'
            assert local == 'Person'

    @patch('boto3.Session')
    async def test_get_local_name_with_slash(self, mock_session):
        """Test extraction of local name from IRI with slash.
        
        This test verifies that:
        1. The _get_local_name method correctly splits IRIs with slash
        2. The prefix and local name are correctly returned
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Act
            iri = 'http://example.org/ontology/Person'
            prefix, local = db._get_local_name(iri)
            
            # Assert
            assert prefix == 'http://example.org/ontology/'
            assert local == 'Person'

    @patch('boto3.Session')
    async def test_get_local_name_invalid(self, mock_session):
        """Test extraction of local name from invalid IRI.
        
        This test verifies that:
        1. The _get_local_name method raises ValueError for IRIs without hash or slash
        2. The error message correctly indicates the issue
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Act & Assert
            with pytest.raises(ValueError, match="Unexpected IRI 'invalid-iri', contains neither '#' nor '/'"):
                db._get_local_name('invalid-iri')

    @patch('boto3.Session')
    async def test_get_rdf_schema_cached(self, mock_session):
        """Test that get_rdf_schema returns cached schema when available.
        
        This test verifies that:
        1. When rdf_schema is already cached, it is returned without making API calls
        2. The cached schema is returned unchanged
        """
        # Arrange
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = 'us-east-1'
        mock_client = MagicMock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Mock _refresh_lpg_schema and _query_sparql to avoid actual API calls during init
        with patch.object(NeptuneDatabase, '_refresh_lpg_schema'), \
             patch.object(NeptuneDatabase, '_query_sparql', return_value={'results': {'bindings': []}}):
            # Create the database instance
            db = NeptuneDatabase(host='test-endpoint')
            
            # Create a mock RDF schema
            mock_rdf_schema = RDFGraphSchema(distinct_prefixes={})
            
            # Set the cached schema
            db.rdf_schema = mock_rdf_schema
            
            # Reset the mocks to verify they're not called
            mock_client.get_rdf_graph_summary.reset_mock()
            
            # Act
            result = db.get_rdf_schema()
            
            # Assert
            assert result == mock_rdf_schema
            mock_client.get_rdf_graph_summary.assert_not_called()