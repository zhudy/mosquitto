FROM gitpod/workspace-full

# Install custom tools, runtime, etc.

#要用到https://github.com/DaveGamble/cJSON.git
RUN sudo chown -R gitpod /usr/local  #放开大发了... 在console上 git clone https://github.com/DaveGamble/cJSON.git，然后 make, make install之

# Apply user-specific settings
# ENV ...
