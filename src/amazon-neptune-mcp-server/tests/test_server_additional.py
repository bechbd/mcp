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
"""Additional tests for the amazon-neptune MCP Server."""

import pytest
from awslabs.amazon_neptune_mcp_server.server import (
    get_graph,
    run_sparql_query,
)
from unittest.mock import MagicMock, patch


@pytest.mark.asyncio
class TestServerEnvironmentVariables:
    """Test class for server environment variable handling."""

    @patch('os.environ.get')
    @patch('awslabs.amazon_neptune_mcp_server.server.NeptuneServer')
    async def test_get_graph_with_custom_port(self, mock_neptune_server, mock_environ_get):
        """Test that get_graph correctly uses a custom port from environment variables.

        This test verifies that:
        1. The NEPTUNE_PORT environment variable is correctly read and converted to an integer
        2. NeptuneServer is initialized with the correct port parameter
        """
        # Arrange
        mock_environ_get.side_effect = lambda key, default=None: {
            'NEPTUNE_ENDPOINT': 'neptune-db://test-endpoint',
            'NEPTUNE_PORT': '8183',  # Custom port
            'NEPTUNE_USE_HTTPS': 'True',
        }.get(key, default)

        # Reset the global _graph variable
        import awslabs.amazon_neptune_mcp_server.server
        awslabs.amazon_neptune_mcp_server.server._graph = None

        mock_server = MagicMock()
        mock_neptune_server.return_value = mock_server

        # Act
        graph = get_graph()

        # Assert
        assert graph == mock_server
        mock_neptune_server.assert_called_once_with(
            'neptune-db://test-endpoint', port=8183, use_https=True
        )

    @patch('os.environ.get')
    @patch('awslabs.amazon_neptune_mcp_server.server.NeptuneServer')
    async def test_get_graph_with_https_variations(self, mock_neptune_server, mock_environ_get):
        """Test that get_graph correctly handles different HTTPS settings.

        This test verifies that:
        1. Different string values for NEPTUNE_USE_HTTPS are correctly interpreted
        2. NeptuneServer is initialized with the correct use_https parameter
        """
        # Test cases for different HTTPS settings
        test_cases = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('1', True),
            ('t', True),
            ('false', False),
            ('False', False),
            ('FALSE', False),
            ('0', False),
            ('f', False),
            ('anything_else', False),
        ]

        mock_server = MagicMock()
        mock_neptune_server.return_value = mock_server

        for https_value, expected_bool in test_cases:
            # Arrange
            mock_environ_get.side_effect = lambda key, default=None: {
                'NEPTUNE_ENDPOINT': 'neptune-db://test-endpoint',
                'NEPTUNE_USE_HTTPS': https_value,
            }.get(key, default)

            # Reset the global _graph variable
            import awslabs.amazon_neptune_mcp_server.server
            awslabs.amazon_neptune_mcp_server.server._graph = None

            # Act
            graph = get_graph()

            # Assert
            assert graph == mock_server
            mock_neptune_server.assert_called_with(
                'neptune-db://test-endpoint', port=8182, use_https=expected_bool
            )
            mock_neptune_server.reset_mock()

    @patch('awslabs.amazon_neptune_mcp_server.server.get_graph')
    async def test_run_sparql_query_logging(self, mock_get_graph, caplog):
        """Test that run_sparql_query correctly logs the query.

        This test verifies that:
        1. The SPARQL query is logged at INFO level
        2. The query_sparql method is called with the correct query
        """
        # Arrange
        mock_graph = MagicMock()
        mock_result = {'results': [{'s': 'http://example.org/subject'}]}
        mock_graph.query_sparql.return_value = mock_result
        mock_get_graph.return_value = mock_graph
        query = 'SELECT * WHERE { ?s ?p ?o } LIMIT 1'

        # Act
        with patch('awslabs.amazon_neptune_mcp_server.server.logger.info') as mock_logger:
            result = run_sparql_query(query)

        # Assert
        assert result == mock_result
        mock_graph.query_sparql.assert_called_once_with(query)
        mock_logger.assert_called_once_with(f'query: {query}')


@pytest.mark.asyncio
class TestServerResourceFunctions:
    """Test class for server resource functions."""

    @patch('awslabs.amazon_neptune_mcp_server.server.get_graph')
    async def test_get_rdf_schema_resource_error_handling(self, mock_get_graph):
        """Test error handling in get_rdf_schema_resource.

        This test verifies that:
        1. When an exception occurs in the graph's rdf_schema method, it's propagated
        """
        # Arrange
        mock_graph = MagicMock()
        mock_graph.rdf_schema.side_effect = Exception("Test error")
        mock_get_graph.return_value = mock_graph

        # Act & Assert
        from awslabs.amazon_neptune_mcp_server.server import get_rdf_schema_resource
        with pytest.raises(Exception, match="Test error"):
            get_rdf_schema_resource()

    @patch('awslabs.amazon_neptune_mcp_server.server.get_graph')
    async def test_get_propertygraph_schema_resource_error_handling(self, mock_get_graph):
        """Test error handling in get_propertygraph_schema_resource.

        This test verifies that:
        1. When an exception occurs in the graph's propertygraph_schema method, it's propagated
        """
        # Arrange
        mock_graph = MagicMock()
        mock_graph.propertygraph_schema.side_effect = Exception("Test error")
        mock_get_graph.return_value = mock_graph

        # Act & Assert
        from awslabs.amazon_neptune_mcp_server.server import get_propertygraph_schema_resource
        with pytest.raises(Exception, match="Test error"):
            get_propertygraph_schema_resource()

    @patch('awslabs.amazon_neptune_mcp_server.server.get_graph')
    async def test_get_status_resource_error_handling(self, mock_get_graph):
        """Test error handling in get_status_resource.

        This test verifies that:
        1. When an exception occurs in the graph's status method, it's propagated
        """
        # Arrange
        mock_graph = MagicMock()
        mock_graph.status.side_effect = Exception("Test error")
        mock_get_graph.return_value = mock_graph

        # Act & Assert
        from awslabs.amazon_neptune_mcp_server.server import get_status_resource
        with pytest.raises(Exception, match="Test error"):
            get_status_resource()
