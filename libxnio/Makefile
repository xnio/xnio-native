#
#
#

CFILES=$(sort $(wildcard src/*.c))
LINUX_x86_64_OFILES=$(patsubst src/%.c,linux-x86_64/%.o,$(CFILES))
LINUX_x32_OFILES=$(patsubst src/%.c,linux-x32/%.o,$(CFILES))
LINUX_i686_OFILES=$(patsubst src/%.c,linux-i686/%.o,$(CFILES))

ALL_OFILES=$(LINUX_x86_64_OFILES) $(LINUX_x32_OFILES) $(LINUX_i686_OFILES)

ALL_TARGETS=linux-x86_64/libxnio.so linux-i686/libxnio.so linux-x32/libxnio.so

STRIP=strip

default: linux

linux: linux-x86_64/libxnio.so linux-i686/libxnio.so

all: default

clean:
	@echo '       rm ' $(ALL_OFILES) $(ALL_TARGETS)
	@$(RM) -f $(ALL_OFILES) $(ALL_TARGETS)

$(ALL_OFILES): Makefile

linux-x86_64/%.o: src/%.c
	@echo '  compile ' $@
	@$(CC) -m64 $< -Iinclude -std=gnu99 -fPIC -g -c -o $@ -O2 -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/linux/include -Wall -Werror

linux-x86_64/libxnio.so: $(LINUX_x86_64_OFILES)
	@echo '     link ' $@
	@$(CC) -m64 -shared $(LINUX_x86_64_OFILES) -o $@
	@echo '    strip ' $@
	@$(STRIP) $@

linux-x32/%.o: src/%.c
	@echo '  compile ' $@
	@$(CC) -mx32 $< -Iinclude -std=gnu99 -fPIC -g -c -o $@ -O2 -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/linux/include -Wall -Werror

linux-x32/libxnio.so: $(LINUX_x32_OFILES)
	@echo '     link ' $@
	@$(CC) -mx32 -shared $(LINUX_x32_OFILES) -o $@
	@echo '    strip ' $@
	@$(STRIP) $@

linux-i686/%.o: src/%.c
	@echo '  compile ' $@
	@$(CC) -m32 $< -Iinclude -std=gnu99 -fPIC -g -c -o $@ -O2 -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/linux/include -Wall -Werror

linux-i686/libxnio.so: $(LINUX_i686_OFILES)
	@echo '     link ' $@
	@$(CC) -m32 -shared $(LINUX_i686_OFILES) -o $@
	@echo '    strip ' $@
	@$(STRIP) $@

.PHONY: clean all linux default
