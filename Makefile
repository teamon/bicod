# OPTS = -fwarn-incomplete-patterns
RUNHS = runhaskell $(OPTS)

run:
	$(RUNHS) Database/Bicod/Main.hs

autorun:
	rerun -p "**/*.hs" "make run"

test:
	$(RUNHS) test/Spec.hs

autotest:
	rerun -p "**/*.hs" "make test"
