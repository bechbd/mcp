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
"""Tests for the awslabs.amazon-neptune-mcp-server package."""

import awslabs.amazon_neptune_mcp_server
import importlib
import re


class TestInit:
    """Tests for the __init__.py module."""

    def test_version(self):
        """Test that __version__ is defined and follows semantic versioning."""
        # Import the module
        import awslabs.amazon_neptune_mcp_server

        # Check that __version__ is defined
        assert hasattr(awslabs.amazon_neptune_mcp_server, '__version__')

        # Check that __version__ is a string
        assert isinstance(awslabs.amazon_neptune_mcp_server.__version__, str)

        # Check that __version__ follows semantic versioning (major.minor.patch)
        version_pattern = r'^\d+\.\d+\.\d+$'
        assert re.match(version_pattern, awslabs.amazon_neptune_mcp_server.__version__), (
            f"Version '{awslabs.amazon_neptune_mcp_server.__version__}' does not follow semantic versioning"
        )

    def test_module_reload(self):
        """Test that the module can be reloaded."""
        # Import the module
        import awslabs.amazon_neptune_mcp_server

        # Store the original version
        original_version = awslabs.amazon_neptune_mcp_server.__version__

        # Reload the module
        importlib.reload(awslabs.amazon_neptune_mcp_server)

        # Check that the version is still the same
        assert awslabs.amazon_neptune_mcp_server.__version__ == original_version

    def test_version_format(self):
        """Test that the package version follows semantic versioning.

        This test verifies that:
        1. The __version__ attribute exists
        2. The version follows the semantic versioning format (MAJOR.MINOR.PATCH)
        """
        # Arrange
        version = awslabs.amazon_neptune_mcp_server.__version__
        semver_pattern = r'^\d+\.\d+\.\d+$'

        # Act & Assert
        assert hasattr(awslabs.amazon_neptune_mcp_server, '__version__')
        assert re.match(semver_pattern, version) is not None, (
            f'Version {version} does not follow semantic versioning'
        )

    def test_package_docstring(self):
        """Test that the package has a docstring.

        This test verifies that:
        1. The package has a docstring
        2. The docstring is not empty
        """
        # Act & Assert
        assert awslabs.amazon_neptune_mcp_server.__doc__ is not None
        assert len(awslabs.amazon_neptune_mcp_server.__doc__) > 0
