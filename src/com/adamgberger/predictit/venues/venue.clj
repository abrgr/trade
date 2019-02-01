(ns com.adamgberger.predictit.venues.venue
  (:gen-class))

(defprotocol Venue
  (id [venue])
  (current-available-balance [venue])
  (available-markets [venue])
  (positions [venue])
  (contracts [venue market-id full-market-url])
  (orders [venue market-id full-market-url contract-id])
  (monitor-order-book [venue market-id full-market-url contract-id])
  (submit-order [venue mkt-id contract-id trade-type qty price])
  (cancel-order [mkt-id order-id]))
