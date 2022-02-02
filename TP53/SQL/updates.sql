-- update.sql

-- FIX for discripencies between SomaticView View Script and SomaticView Data (in Table)
-- Update Mutation data to change lower case 'na' to 'NA'

UPDATE `isb-cgc-tp53-dev.P53_data.MUTATION`
SET Mutant_codon = 'NA'
WHERE Mutant_codon = 'na'