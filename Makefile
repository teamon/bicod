# OPTS = +RTS -prof -xc --RTS
RUNHS = runghc $(OPTS)

all:
	$(RUNHS) Setup.hs configure
	$(RUNHS) Setup.hs build

run:
	$(RUNHS) Database/Bicod/Main.hs

autorun:
	rerun -p "**/*.hs" "make run"

test: test/Spec.hs
	$(RUNHS) test/Spec.hs

autotest:
	rerun -p "**/*.hs" "make test"
