-- WS7g (#36) — grant the test user rights to create/use the per-language namespaced databases.
--
-- The coordinated live-DB pass isolates each language runtime in its OWN MySQL database
-- (scp_py / scp_go / scp_php / scp_rust) so all four share ONE docker stack without table
-- cross-contamination. The base init user (testuser) only owns `testdb`; this override-mounted
-- init grants it ALL on the `scp_%` database family. (Mounted only by docker-compose.livedb.yml.)
GRANT ALL PRIVILEGES ON `scp\_%`.* TO 'testuser'@'%';
FLUSH PRIVILEGES;
