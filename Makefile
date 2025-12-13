PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
PERL ?= $(filter /%,$(shell /bin/sh -c 'type perl'))
VERSION := 0.1.2025.12.13
PROGRAM := kafssdbinfo kafssdedup kafssfreq kafssindex kafsspart kafsspreload kafsssubset kafsssearch kafsssearchclient kafssstore
SERVER := kafsssearchserver.pl kafsssearchserver.fcgi kafsssearchserver.psgi

all: $(PROGRAM) version-server

%: %.pl
	echo '#!'$(PERL) > $@
	echo "use lib '$(PREFIX)/share/kafsss/lib/perl5';" >> $@
	tail -n +2 $< | perl -npe 's/__VERSION__/$(VERSION)/g' >> $@

version-server:
	perl -i -npe 's/__VERSION__/$(VERSION)/g' $(SERVER)

install: $(PROGRAM)
	chmod 755 $^
	mkdir -p $(BINDIR)
	cp $^ $(BINDIR)
	mkdir -p $(PREFIX)/share/kafsss
	mkdir -p $(PREFIX)/share/kafsss/lib/perl5

clean:
	rm -f $(PROGRAM)
	perl -i -npe 's/$(VERSION)/__VERSION__/g' $(SERVER)

.PHONY: all install clean version-server