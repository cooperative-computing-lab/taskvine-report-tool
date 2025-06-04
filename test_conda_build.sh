#!/bin/bash

# Test conda build script
echo "Starting conda package build test..."

# Check if conda-build is installed
if ! command -v conda-build &> /dev/null; then
    echo "Installing conda-build..."
    conda install conda-build conda-verify -y
fi

# Build package
echo "Building conda package..."
conda build conda-recipe/ --output-folder dist/

# Check build result
if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    echo "Generated package location:"
    ls -la dist/noarch/taskvine-report-tool-*.tar.bz2
    
    echo ""
    echo "Testing installation..."
    conda install dist/noarch/taskvine-report-tool-*.tar.bz2 -y
    
    echo ""
    echo "Testing import..."
    python -c "import taskvine_report; print('✅ Import successful, version:', taskvine_report.__version__)"
    
    echo ""
    echo "Testing command line tools..."
    vine_parse --help > /dev/null && echo "✅ vine_parse command available"
    vine_report --help > /dev/null && echo "✅ vine_report command available"
    
else
    echo "❌ Build failed"
    exit 1
fi 