"""Cancellation prediction model for campsite availability.

Stage 1: scaffolding — dataset loading, feature engineering, and a
baseline logistic regression.  Advanced features (weather, holidays,
reservation duration) will come in stage 2.

Target variable
---------------
For a campsite observed as ``Reserved`` on check_date D at time T,
will it transition to ``Available`` at any observation before D?

This is the practical question: "is this site worth watching?"
"""
