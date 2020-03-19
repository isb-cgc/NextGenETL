The raw schema files here are to match the output of the scripts/createSchema.py script, as insurance that nothing
has changed since the last run. They do NOT have to match the schemas that live in the BQEcosystem repo.
Specifically, the slidMap schema has type integer for field "sample_type_id", since that is what the script determines
(all the sample types are either a two digit code, or null). However, the aliqMap schema uses a string for that field
(it used to be an int before around rel16), so we want to keep the two consistent, and allow for non-integer codes
 in the future. So the BQEcosystem repo calls the field a string regardless of the createSchema.py script.