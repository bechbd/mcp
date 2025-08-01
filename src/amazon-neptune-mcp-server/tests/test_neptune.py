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
"""Tests for the NeptuneServer class."""

import pytest
from awslabs.amazon_neptune_mcp_server.models import GraphSchema, RDFGraphSchema
from awslabs.amazon_neptune_mcp_server.neptune import NeptuneServer
from unittest.mock import MagicMock, patch


@pytest.mark.asyncio
class TestNeptuneServer:
    """Test class for the NeptuneServer functionality."""

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_init_neptune_db(self, mock_neptune_db):
        """Test initialization of NeptuneServer with a Neptune Database endpoint.
        This test verifies that:
        1. The Neptune Database endpoint is correctly parsed
        2. NeptuneDatabase is initialized with the correct parameters
        3. The graph attribute is set to the NeptuneDatabase instance.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_neptune_db.return_value = mock_db_instance
        endpoint = 'neptune-db://test-endpoint'

        # Act
        server = NeptuneServer(endpoint, use_https=True, port=8182)

        # Assert
        assert server.graph == mock_db_instance
        mock_neptune_db.assert_called_once_with('test-endpoint', 8182, use_https=True)

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneAnalytics')
    async def test_init_neptune_analytics(self, mock_neptune_analytics):
        """Test initialization of NeptuneServer with a Neptune Analytics endpoint.
        This test verifies that:
        1. The Neptune Analytics endpoint is correctly parsed
        2. NeptuneAnalytics is initialized with the correct graph ID
        3. The graph attribute is set to the NeptuneAnalytics instance.
        """
        # Arrange
        mock_analytics_instance = MagicMock()
        mock_neptune_analytics.return_value = mock_analytics_instance
        endpoint = 'neptune-graph://test-graph-id'

        # Act
        server = NeptuneServer(endpoint)

        # Assert
        assert server.graph == mock_analytics_instance
        mock_neptune_analytics.assert_called_once_with('test-graph-id')

    async def test_init_invalid_endpoint_format(self):
        """Test that NeptuneServer initialization fails with an invalid endpoint format.
        This test verifies that:
        1. When an endpoint with an invalid format is provided, a ValueError is raised
        2. The error message correctly indicates the expected format.
        """
        # Act & Assert
        with pytest.raises(
            ValueError,
            match='You must provide an endpoint to create a NeptuneServer as either neptune-db',
        ):
            NeptuneServer('invalid-endpoint')

    async def test_init_empty_endpoint(self):
        """Test that NeptuneServer initialization fails with an empty endpoint.
        This test verifies that:
        1. When an empty endpoint is provided, a ValueError is raised
        2. The error message correctly indicates that an endpoint is required.
        """
        # Act & Assert
        with pytest.raises(
            ValueError, match='You must provide an endpoint to create a NeptuneServer'
        ):
            NeptuneServer('')

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_status_available(self, mock_neptune_db):
        """Test that status() returns "Available" when the database is available.
        This test verifies that:
        1. A test query is executed to check database availability
        2. When the query succeeds, "Available" is returned.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_db_instance.query_opencypher.return_value = {'result': 1}
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        status = server.status()

        # Assert
        assert status == 'Available'
        mock_db_instance.query_opencypher.assert_called_once_with('RETURN 1', None)

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_status_unavailable(self, mock_neptune_db):
        """Test that status() returns "Unavailable" when the database is unavailable.
        This test verifies that:
        1. A test query is executed to check database availability
        2. When the query fails, "Unavailable" is returned
        3. The exception is properly handled.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_db_instance.query_opencypher.side_effect = Exception('Connection error')
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        status = server.status()

        # Assert
        assert status == 'Unavailable'
        mock_db_instance.query_opencypher.assert_called_once_with('RETURN 1', None)

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_propertygraph_schema(self, mock_neptune_db):
        """Test that propertygraph_schema() correctly returns the property graph schema.
        This test verifies that:
        1. The get_lpg_schema method is called on the graph instance
        2. The result from the graph's get_lpg_schema method is returned unchanged.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_schema = GraphSchema(nodes=[], relationships=[], relationship_patterns=[])
        mock_db_instance.get_lpg_schema.return_value = mock_schema
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        schema = server.propertygraph_schema()

        # Assert
        assert schema == mock_schema
        mock_db_instance.get_lpg_schema.assert_called_once()

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_rdf_schema(self, mock_neptune_db):
        """Test that rdf_schema() correctly returns the RDF graph schema.
        This test verifies that:
        1. The get_rdf_schema method is called on the graph instance
        2. The result from the graph's get_rdf_schema method is returned unchanged.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_schema = RDFGraphSchema(distinct_prefixes={})
        mock_db_instance.get_rdf_schema.return_value = mock_schema
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        schema = server.rdf_schema()

        # Assert
        assert schema == mock_schema
        mock_db_instance.get_rdf_schema.assert_called_once()

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_query_opencypher(self, mock_neptune_db):
        """Test that query_opencypher correctly executes an openCypher query without parameters.
        This test verifies that:
        1. The query_opencypher method is called on the graph instance with the correct query
        2. The result from the graph's query_opencypher method is returned unchanged.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_result = {'results': [{'n': {'id': '1'}}]}
        mock_db_instance.query_opencypher.return_value = mock_result
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        result = server.query_opencypher('MATCH (n) RETURN n LIMIT 1')

        # Assert
        assert result == mock_result
        mock_db_instance.query_opencypher.assert_called_once_with(
            'MATCH (n) RETURN n LIMIT 1', None
        )

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_query_opencypher_with_parameters(self, mock_neptune_db):
        """Test that query_opencypher correctly executes an openCypher query with parameters.
        This test verifies that:
        1. The query_opencypher method is called on the graph instance with the correct query and parameters
        2. The result from the graph's query_opencypher method is returned unchanged.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_result = {'results': [{'n': {'id': '1'}}]}
        mock_db_instance.query_opencypher.return_value = mock_result
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')
        parameters = {'id': '1'}

        # Act
        result = server.query_opencypher('MATCH (n) WHERE n.id = $id RETURN n', parameters)

        # Assert
        assert result == mock_result
        mock_db_instance.query_opencypher.assert_called_once_with(
            'MATCH (n) WHERE n.id = $id RETURN n', parameters
        )

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_query_gremlin(self, mock_neptune_db):
        """Test that query_gremlin correctly executes a Gremlin query.

        This test verifies that:
        1. The query_gremlin method is called on the graph instance with the correct query
        2. The result from the graph's query_gremlin method is returned unchanged.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_result = {'results': [{'id': '1'}]}
        mock_db_instance.query_gremlin.return_value = mock_result
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        result = server.query_gremlin('g.V().limit(1)')

        # Assert
        assert result == mock_result
        mock_db_instance.query_gremlin.assert_called_once_with('g.V().limit(1)')

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_query_sparql(self, mock_neptune_db):
        """Test that query_sparql correctly executes a SPARQL query.

        This test verifies that:
        1. The query_sparql method is called on the graph instance with the correct query
        2. The result from the graph's query_sparql method is returned unchanged.
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_result = {
            'results': [
                {
                    's': 'http://example.org/subject',
                    'p': 'http://example.org/predicate',
                    'o': 'Object',
                }
            ]
        }
        mock_db_instance.query_sparql.return_value = mock_result
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        result = server.query_sparql('SELECT * WHERE { ?s ?p ?o } LIMIT 1')

        # Assert
        assert result == mock_result
        mock_db_instance.query_sparql.assert_called_once_with(
            'SELECT * WHERE { ?s ?p ?o } LIMIT 1'
        )

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_init_with_trailing_slash(self, mock_neptune_db):
        """Test initialization of NeptuneServer with a Neptune Database endpoint that has a trailing slash.

        This test verifies that:
        1. The Neptune Database endpoint with trailing slash is correctly parsed
        2. NeptuneDatabase is initialized with the correct parameters
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_neptune_db.return_value = mock_db_instance
        endpoint = 'neptune-db://test-endpoint/'

        # Act
        server = NeptuneServer(endpoint, use_https=True, port=8182)

        # Assert
        assert server.graph == mock_db_instance
        mock_neptune_db.assert_called_once_with('test-endpoint', 8182, use_https=True)

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneAnalytics')
    async def test_init_analytics_with_trailing_slash(self, mock_neptune_analytics):
        """Test initialization of NeptuneServer with a Neptune Analytics endpoint that has a trailing slash.

        This test verifies that:
        1. The Neptune Analytics endpoint with trailing slash is correctly parsed
        2. NeptuneAnalytics is initialized with the correct graph ID
        """
        # Arrange
        mock_analytics_instance = MagicMock()
        mock_neptune_analytics.return_value = mock_analytics_instance
        endpoint = 'neptune-graph://test-graph-id/'

        # Act
        server = NeptuneServer(endpoint)

        # Assert
        assert server.graph == mock_analytics_instance
        mock_neptune_analytics.assert_called_once_with('test-graph-id')

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_query_gremlin_error_handling(self, mock_neptune_db):
        """Test error handling in query_gremlin.

        This test verifies that:
        1. When an exception occurs in the graph's query_gremlin method, it's propagated
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_db_instance.query_gremlin.side_effect = Exception('Test error')
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act & Assert
        with pytest.raises(Exception, match='Test error'):
            server.query_gremlin('g.V().limit(1)')

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_query_sparql_error_handling(self, mock_neptune_db):
        """Test error handling in query_sparql.

        This test verifies that:
        1. When an exception occurs in the graph's query_sparql method, it's propagated
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_db_instance.query_sparql.side_effect = Exception('Test error')
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act & Assert
        with pytest.raises(Exception, match='Test error'):
            server.query_sparql('SELECT * WHERE { ?s ?p ?o }')

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_propertygraph_schema_error_handling(self, mock_neptune_db):
        """Test error handling in propertygraph_schema.

        This test verifies that:
        1. When an exception occurs in the graph's get_lpg_schema method, it's propagated
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_db_instance.get_lpg_schema.side_effect = Exception('Test error')
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act & Assert
        with pytest.raises(Exception, match='Test error'):
            server.propertygraph_schema()

    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_rdf_schema_error_handling(self, mock_neptune_db):
        """Test error handling in rdf_schema.

        This test verifies that:
        1. When an exception occurs in the graph's get_rdf_schema method, it's propagated
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_db_instance.get_rdf_schema.side_effect = Exception('Test error')
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act & Assert
        with pytest.raises(Exception, match='Test error'):
            server.rdf_schema()

    @patch('awslabs.amazon_neptune_mcp_server.neptune.logger')
    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_logging_on_initialization(self, mock_neptune_db, mock_logger):
        """Test that initialization logs appropriate messages.

        This test verifies that:
        1. Debug log messages are created during initialization
        """
        # Arrange
        mock_db_instance = MagicMock()
        mock_neptune_db.return_value = mock_db_instance

        # Act
        NeptuneServer('neptune-db://test-endpoint')

        # Assert
        mock_logger.debug.assert_called_with(
            'Creating Neptune Database session for %s', 'test-endpoint'
        )

    @patch('awslabs.amazon_neptune_mcp_server.neptune.logger')
    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneAnalytics')
    async def test_logging_on_analytics_initialization(self, mock_neptune_analytics, mock_logger):
        """Test that initialization logs appropriate messages for Analytics.

        This test verifies that:
        1. Debug log messages are created during initialization of Analytics
        """
        # Arrange
        mock_analytics_instance = MagicMock()
        mock_neptune_analytics.return_value = mock_analytics_instance

        # Act
        NeptuneServer('neptune-graph://test-graph-id')

        # Assert
        mock_logger.debug.assert_called_with(
            'Creating Neptune Graph session for %s', 'neptune-graph://test-graph-id'
        )

    @patch('awslabs.amazon_neptune_mcp_server.neptune.logger')
    @patch('awslabs.amazon_neptune_mcp_server.neptune.NeptuneDatabase')
    async def test_status_logs_exception(self, mock_neptune_db, mock_logger):
        """Test that status() logs exceptions.

        This test verifies that:
        1. When an exception occurs in the query_opencypher method, it's logged
        2. The status method returns "Unavailable"
        """
        # Arrange
        mock_db_instance = MagicMock()
        test_exception = Exception('Connection error')
        mock_db_instance.query_opencypher.side_effect = test_exception
        mock_neptune_db.return_value = mock_db_instance

        server = NeptuneServer('neptune-db://test-endpoint')

        # Act
        status = server.status()

        # Assert
        assert status == 'Unavailable'
        mock_logger.exception.assert_called_once_with('Could not get status for Neptune instance')
