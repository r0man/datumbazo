-- Deploy create-twitter-tweets-users

BEGIN;

CREATE TABLE twitter.tweets_users(
  tweet_id bigint NOT NULL REFERENCES twitter.tweets(id) ON DELETE CASCADE,
  user_id bigint NOT NULL REFERENCES twitter.users(id) ON DELETE CASCADE,
  CONSTRAINT twitter_tweets_users_tweet_id_user_id PRIMARY KEY(tweet_id, user_id)
);

COMMIT;
