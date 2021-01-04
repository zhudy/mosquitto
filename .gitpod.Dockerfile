FROM gitpod/workspace-full

# Install custom tools, runtime, etc.

#要用到https://github.com/DaveGamble/cJSON.git
#放开大发了... 在console上 git clone https://github.com/DaveGamble/cJSON.git，然后 make, make install之
#之前的没管上用处？改成下面这个看看

RUN sudo chown -R gitpod:gitpod /usr/local  

#再试试权限
RUN sudo chmod -R 777 /usr/local

# Apply user-specific settings
# ENV ...
