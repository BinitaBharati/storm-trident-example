(defproject storm-trident-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/main/clj"]
  :java-source-paths ["src/main/java" "src/test/java"]
  :dependencies [
                  [org.clojure/clojure "1.5.1"]
                  [org.apache.storm/storm-core "0.9.2-incubating"]
                  ;[org.slf4j/slf4j-api "1.7.7"]
                  ;[org.slf4j/slf4j-log4j12 "1.7.7"]
                  [org.twitter4j/twitter4j-core "4.0.2"]
                  [org.twitter4j/twitter4j-stream "4.0.2"]
                  [com.googlecode.json-simple/json-simple "1.1.1"]
                  [org.jmock/jmock-legacy "2.6.0"]
                  [org.jmock/jmock-junit4 "2.6.0"]
                  [redis.clients/jedis "2.5.2"]
                  [org.apache.tika/tika-parsers "1.5"]
                  [org.apache.lucene/lucene-analyzers "3.6.2"]
                  [org.apache.lucene/lucene-spellchecker "3.6.2"]
                  [edu.washington.cs.knowitall/morpha-stemmer "1.0.5"]
                  [trident-cassandra "0.0.1-wip2"]
                  [commons-io/commons-io "2.4"]
                 ]
  ;:profiles {:provided {:dependencies [
                                  ;   [org.apache.storm/storm-core "0.9.2-incubating"]
                                 ; ]}}
  )
