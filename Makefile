PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
PERL ?= $(filter /%,$(shell /bin/sh -c 'type perl'))
VERSION := 0.1.2026.02.02
PROGRAM := kafssdbinfo kafssdedup kafssfreq kafssindex kafsspart kafsspreload kafsssubset kafsssearch kafsssearchclient kafssstore
SERVER := kafsssearchserver.psgi

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

installserver: version-server
ifndef DESTDIR
	$(error DESTDIR is not set. Usage: make installserver DESTDIR=/path/to/install)
endif
	mkdir -p $(DESTDIR)
	cp $(SERVER) $(DESTDIR)
	chmod 755 $(DESTDIR)/$(SERVER)

clean:
	rm -f $(PROGRAM)
	perl -i -npe 's/$(VERSION)/__VERSION__/g' $(SERVER)

.PHONY: all install installserver clean version-server