# OPTS = -fwarn-incomplete-patterns
RUNHS = runhaskell $(OPTS)

run:
	$(RUNHS) Bicod.hs

autorun:
	rerun -p "**/*.hs" "make run"

test:
	$(RUNHS) Spec.hs

autotest:
	rerun -p "**/*.hs" "make test"
