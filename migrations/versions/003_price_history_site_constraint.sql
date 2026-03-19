-- Add missing site CHECK constraint on price_history table
ALTER TABLE price_history
    ADD CONSTRAINT chk_price_history_site
    CHECK (site IN ('danawa', 'compuzone', 'pc_estimate'));
