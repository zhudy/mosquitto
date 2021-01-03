FROM gitpod/workspace-full

# Install custom tools, runtime, etc.
RUN sudo apt-get update \
    && sudo apt-get install -y \
        libjsoncpp-dev \
    && sudo rm -rf /var/lib/apt/lists/*

# Apply user-specific settings
# ENV ...
