PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
PERL ?= $(filter /%,$(shell /bin/sh -c 'type perl'))
VERSION := 0.1.2025.07.13
YEAR := 2025
PROGRAM := kafssdbinfo kafssdedup kafssfreq kafssindex kafsssubset kafsssearch kafsssearchclient kafssstore

all: $(PROGRAM)

%: %.pl
	echo '#!'$(PERL) > $@
	echo "use lib '$(PREFIX)/share/kafsss/lib/perl5';" >> $@
	tail -n +2 $< >> $@

install: $(PROGRAM)
	chmod 755 $^
	mkdir -p $(BINDIR)
	cp $^ $(BINDIR)
	mkdir -p $(PREFIX)/share/kafsss
	mkdir -p $(PREFIX)/share/kafsss/lib/perl5

clean:
	rm -f $(PROGRAM)

.PHONY: all install clean