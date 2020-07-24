(ns devchk.core
  (:require
   [clojure.data.csv :as csv]
   [clojure.tools.namespace.repl]
   [clojure.java.shell :as shell]
   [clojure.string :refer [split]]
   [clojure.core.async :as async :refer [<! >! go-loop]]
   [cheshire.core :as json]))

(defn abs-path [path] (-> path java.io.File. .getAbsolutePath str))

;; number of parallel image processors to run, one per-core
(def num-processors (-> (Runtime/getRuntime) .availableProcessors))

(def use-prod-iiif true) ;; use the `iiif--prod` server
(def clear-image-cache true) ;; set to false to preserve image cache (good for debugging)

(def cache-root "") ;; "/ext/devchk/", when you need a different cache dir (ensure trailing slash)
(def cache-dir (-> cache-root (str "cache") abs-path)) ;; "/path/to/cache"
(def image-cache-dir (-> cache-root (str "image-cache") abs-path)) ;; "/path/to/cache/image-cache"
(def log-cache-dir (-> cache-root (str "logs") abs-path)) ;; "/path/to/cache/logs"

;; create our cache dirs
(run! (fn [path] (.mkdirs (java.io.File. path))) [cache-dir image-cache-dir log-cache-dir])

(def image-chan
  "this is where we'll put the image data we find that needs processing."
  (async/chan 10))

(def results-chan
  "this is where we'll put the results of processing images."
  (async/chan 10))

(def stats
  "this is where we accumulate some ongoing stats about the state of the program as it's running."
  (atom {:num-images 0     ;; number of images found
         :num-processed 0  ;; number of images processed
         :num-deviations 0 ;; number of deviations we've found
         :num-errors 0}))  ;; number of errors encountered processing images

(defn parse-json-string [string] (json/parse-string string true))
(defn log-file [level] (str log-cache-dir "/" (-> level name (str ".json"))))

(defn read-report
  "if a report exists, create an index of uri->md5 results so we don't have to process them again"
  []
  (let [report-file (-> :report log-file java.io.File.)]
    (when (.exists report-file)
      (with-open [rdr (clojure.java.io/reader report-file)]
        (into {} (mapv (fn [result]
                         (let [result (:message (parse-json-string result))]
                           [(:uri (:source result)) (:md5 result)]))
                       (line-seq rdr)))))))

(def report-idx (read-report))

;; utils

(defn increment
  "helper function that bumps stats"
  [what]
  (swap! stats update-in [what] inc))

(let [lock (Object.)]
  (defn stderr
    "synchronised printing to stderr. ensures everybody gets a line on stdout to themselves."
    [& args]
    (locking lock
      (binding [*out* *err*]
        (apply println args)
        (flush)))))

(let [lock (Object.)]
  (defn log
    "synchronised file writing. ensures everybody gets a line to themselves"
    [level & args]
    (locking lock
      (let [filename (log-file level) ;; "/path/to/cache/logs/debug.json"
            extra (rest args)
            msg (json/generate-string (if (not (empty? extra))
                                        {:message (first args) :extra extra}
                                        {:message (first args)}))]
        (spit filename (str msg "\n") :append true)))))

(defn fs-safe-cache-key
  "safely encode a URI to something that can live cached on the filesystem"
  [url]
  (let [enc (java.util.Base64/getUrlEncoder)]
    (->> url str .getBytes (.encodeToString enc) str)))

(defn spy
  "print the value of `x` and then return without modifying."
  [x]
  (stderr "(spying)" x) x)

(defn sh
  "calls `shell/sh` with given `args` and records a friendly version of the command in the debug log."
  [& args]
  (log :debug (str "$ " (clojure.string/join " " args)))
  (apply shell/sh args))

(defn GET
  "fetch url, write to cache, returns textual body"
  [url]
  (let [output-file (str cache-dir "/" (fs-safe-cache-key url))]
    (if (.exists (java.io.File. output-file))
      (slurp output-file)
      (let [{:keys [:exit :err :out]} (sh "curl" "-sS" url)]
        (log :debug (format "cache miss for '%s': %s" url output-file))
        (if (zero? exit)
          (do (spit output-file out) out)
          (log :error err))))))

(defn GET-binary
  "fetch url, write bytes to given `output-file`, returns `output-file` on success or `nil` on error."
  [url output-file]
  (if (and (.exists (java.io.File. output-file))
           (> (.length (java.io.File. output-file)) 0))
    output-file
    (let [{:keys [:exit :err]} (sh "curl" "--retry" "2" "-sS" url "--output" output-file)]
      (log :debug (format "cache miss for '%s': %s" url output-file))
      (if (zero? exit)
        output-file
        (log :error err)))))

(defn url?
  "predicate, returns `true` if given value can be parsed as a URL"
  [x]
  (try
    (java.net.URL. x)
    true
    (catch Exception e
      false)))

(defn msid?
  "predicate, returns `true` if given value can be parsed as an elife manuscript ID (msid)"
  [x]
  (try
    (-> x java.lang.Integer/valueOf pos-int?)
    (catch Exception e
      false)))

(defn delete-file
  "deletes a file if it exists and is a file"
  [path]
  (when-let [obj (-> path java.io.File.)]
    (when (.isFile obj)
      (.delete obj))))

;; image discovery and extraction

(defn find-images
  "visits everything in given `data` struct (article-json or a sub-structure of it), looking for images.
  when an image is found it is added to `image-chan` for processing."
  [data]
  (let [pred (fn [val]
               (and (map? val)
                    (-> val :type (= "image"))))

        extractor (fn [val]
                    (let [image (:image val)
                          ;; {:label ... :title ... :id ... :size {...} :source ...}
                          ;; for example:
                          ;; {:label "Figure 11",
                          ;;  :title "Strategy to fit Gompertz function parameters on a simulated growth curve with noise added.",
                          ;;  :id "fig11",
                          ;;  :size {:width 1412, :height 624},
                          ;;  :source {:mediaType "image/jpeg",
                          ;;           :uri "https://iiif.elifesciences.org/lax:56613%2Felife-56613-fig11-v2.tif/full/full/0/default.jpg",
                          ;;           :filename "elife-56613-fig11-v2.jpg"}}
                          struct (merge (select-keys val [:label :title :id])
                                        (select-keys image [:size :source]))]
                      (async/>!! image-chan struct)
                      (increment :num-images)))]
    (if (pred data)
      ;; we've found some images! no need to recurse any deeper
      (extractor data)
      ;; we didn't find any images at this level, descend deeper
      (cond
        ;; if we're a map, visit each key+val pair, testing each `val`
        (map? data) (run! (fn [[key val]]
                            [key (find-images val)]) data)
        ;; if we're a list of data, test each item in list
        (sequential? data) (run! find-images data)
        ;; scalar value probably, just exit
        :else nil))))

(defn get-article
  "given the manuscript ID `msid`, downloads the latest version of an elife article"
  [msid]
  (let [article-url (str "https://api.elifesciences.org/articles/" msid)]
    (-> article-url GET parse-json-string)))

(defn articles
  "downloads a report from the Observer project and extracts the manuscript IDs of *all* published articles"
  []
  (let [url "https://observer.elifesciences.org/report/published-article-index.csv"
        csv-data (doall (csv/read-csv (GET url)))
        article-list (->> csv-data rest (map first)
                          (take 20) ;; download N articles. disable this to download *all* articles
                          (map get-article))]

    ;; images are embedded throughout the article-json. we need to visit every value and look for these:
    ;; {"assets" [
    ;;     {"caption" [{"text" "...", "type" "paragraph"}],
    ;;      "id" "fig2",
    ;;      "image" {"alt" "",
    ;;               "uri" "https://iiif.elifesciences.org/lax:56613%2Felife-56613-fig2-v2.tif",
    ;;               "size" {"width" 3605,
    ;;                       "height" 2958},
    ;;               "source" {"mediaType" "image/jpeg",
    ;;                         "uri" "https://iiif.elifesciences.org/lax:56613%2Felife-56613-fig2-v2.tif/full/full/0/default.jpg",
    ;;                         "filename" "elife-56613-fig2-v2.jpg"}},
    ;;      "label" "Figure 2",
    ;;      "title" "Growth of Mtb strain H37Rv during exposure to ofloxacin.",
    ;;      "type" "image"}]}

    (run! find-images article-list))
  nil)

(defn find-all
  "finds sources of iiif data and extracts them. Results are put on the `image-chan` for processing."
  []
  (async/go
    (articles)
    ;; (digests)
    ;; no more data to scrape, signal the channel to close
    (async/close! image-chan)))

(defn find-single-image
  "adds a single iiif image to the `image-chan` for processing."
  [iiif-url]
  (async/put! image-chan {:label "unknown label"
                          :title "unknown title"
                          :id "unknown"
                          :size {:width 0 :height 0}
                          :source {:uri iiif-url}})
  (increment :num-images))

(defn find-single-article
  "adds all of the images in a single elife article to the `image-chan` for processing."
  [msid]
  (stderr "looking for article" msid)
  (async/go
    (find-images (get-article msid))))

;; image processing

(defn compare-images
  "given two images, calls ImageMagick with the right incantation.
  the call to ImageMagick is wrapped in a 'timeout' of a 20 seconds with a --kill after 40 seconds.
  ImageMagick can die and take an underpowered machine with it if we don't stop it."
  [image-path-1 image-path-2 & [attempt]]
  (let [comparison-file (str (java.io.File/createTempFile "devchk" ".jpg"))

        {:keys [exit err out]}
        (sh "timeout" "--kill-after=40s" "20s"
            ;;"magick" "compare" "-metric" "pae" ;; IM 7 only. ubuntu 18.04 still on IM 6
            "compare" "-metric" "pae"
            "-quiet"
            image-path-1 image-path-2 comparison-file)

        ;; 'pae' is Peak Absolute Error, a value between 0 and 1 of the maximum difference between two pixels
        ;; 0 is perfectly identical and 1 is completely different
        ;; setting the fuzz factor to the `pae` value as a percentage will get you a successful result
        [_ pae] (rest (re-matches #"(\d+) \((\d\.\d+)\)" err))
        pae (some-> pae java.lang.Float/valueOf)]
    
    (cond
      ;; ImageMagick exited with a non-zero result. this is the first attempt, try again.
      (and (> exit 1)
           (= attempt nil)) (compare-images image-path-1 image-path-2 2)

      ;; ImageMagick exited with a non-zero result.
      ;; this is not the first attempt so log the error and give up.
      (> exit 1) (do
                   (increment :num-errors)
                   (log :error "imagemagick return an exit code greater than 1"
                        {:stderr err
                         :stdout out
                         :exit exit
                         :image-1 image-path-1
                         :image-2 image-path-2})))

    {:pae pae
     :cache {:local-comparison-file comparison-file
             :local-original-file image-path-1
             :local-iiif-file image-path-2}}))

(defn image-cache-path
  "images must be stored on the disk long enough to compare them with the iiif derived image.
  there is no guaranteee there won't be a naming conflict so preserve the path structure."
  [image-url]
  (let [path (-> image-url java.net.URL. .getPath java.net.URLDecoder/decode) ;; /digests/55692/digest-55692.tif
        file (-> path java.io.File. .getName) ;; digest-55692.tif
        parent (-> path java.io.File. .getParent) ;; /digests/55692/
        cache-dir-obj (java.io.File. (str image-cache-dir parent))] ;; ./image-cache/digests/55692/
    (when-not (.exists cache-dir-obj)
      (-> cache-dir-obj .mkdirs))
    (str cache-dir-obj "/" file))) ;; ./image-cache/digests/55692/digest-55692.tif`

(defn download-image
  "downloads a binary image, returns path to file within the `image-cache`"
  [image-url]
  (GET-binary image-url (image-cache-path image-url)))

(defn iiif-to-s3-url
  "converts a iiif URL to an S3 bucket URL where the original image is stored."
  [iiif-url]
  ;; transformation looks like:
  ;;   https://iiif.elifesciences.org/digests/55692%2Fdigest-55692.tif/full/full/0/default.webp
  ;;   /digests/55692%2Fdigest-55692.tif/full/full/0/default.webp
  ;;   /digests/55692/digest-55692.tif/full/full/0/default.webp
  ;;   /digests/55692/digest-55692.tif
  ;;   https://prod-elife-published.s3.amazonaws.com/digests/55692/digest-55692.tif
  (let [strip-from-full (fn [path]
                          (last (re-find #"(.+)?/full/full/.*" path)))
        lax-to-articles (fn [path]
                          (if (clojure.string/starts-with? path "/lax:")
                            (str "/articles/" (subs path 5))
                            path))
        prepend-s3 (fn [path]
                     (str "https://prod-elife-published.s3.amazonaws.com" path))]
    (-> iiif-url
        java.net.URL.
        .getPath
        java.net.URLDecoder/decode
        strip-from-full
        lax-to-articles
        prepend-s3)))

(defn image-meta
  "returns additional image data"
  [image-path]
  (let [img-obj (java.io.File. image-path)]
    {:bytes (.length img-obj)
     :md5 (first (split (:out (sh "md5sum" "--binary" image-path)) #" "))}))

(defn -process-image
  "given an `image`, generates urls, downloads the images, compares them, tacks on extra image data and returns a map of results"
  [image]
  (let [iiif-url (-> image :source :uri)]
    (if (contains? report-idx iiif-url)
      (log :debug "skipping" iiif-url) ;; already have a result for this one

      (let [original-image-url (iiif-to-s3-url iiif-url)
            original-image (download-image original-image-url)
            local-iiif-url (str "http://localhost" (-> iiif-url java.net.URL. .getPath))
            iiif-image (download-image (if use-prod-iiif iiif-url local-iiif-url))]

        (cond
          (not iiif-image) (log :error "problem downloading iiif image:" {:iiif-image iiif-image})
          (not original-image) (log :error "problem downloading original image:" {:original-image original-image})

          :else
          (let [iiif-meta (-> iiif-image image-meta (assoc :local-uri local-iiif-url))
                original-meta (image-meta original-image)

                article-id (->> iiif-url (re-find #"elife\-(\d{5})\-") last java.lang.Integer/valueOf)
                comparison-results (compare-images original-image iiif-image)

                result (merge image original-meta)
                result (merge result {:uri original-image-url :article-id article-id})
                result (update-in result [:source] merge iiif-meta)
                result (merge result comparison-results)]
            result))))))

(defn process-image
  [image]
  (try
    (-process-image image)
    (catch Exception uncaught-exc
      (log :error "uncaught exception processing image" {:image image :exc (str uncaught-exc)})
      (increment :num-errors))))

(defn image-processor
  "this will take images off of `image-chan` and 'processes' them - runs each image through a series of tests.
  the results are put on the `results-chan`."
  [my-id]
  (stderr "started image processor" my-id)
  (async/go-loop []
    (if-let [image (async/<! image-chan)]
      (let [result (process-image image)]
        (when result
          (log :debug (str "processor:" my-id " - processing image - " (:label image)))
          (async/>! results-chan result))
        (increment :num-processed)
        (recur))

      ;; received nil, no more results on this channel, exit
      (stderr (format "worker %s exiting" my-id))))
  nil)

;; reporting

(defn report
  "this takes results off of `results-chan` and writes a report to stdout as they are processed.
  we do this on the main thread so it blocks us from exiting until we have no more results left."
  []
  (loop []
    (when-let [result (async/<!! results-chan)]
      ;; check for deviations
      (if (or (= (:pae result) 1)
              (= (:pae result) 1.0))
        (increment :num-deviations)

        ;; no errors to investigate, delete the cached files
        (when clear-image-cache
          (some-> result :cache :local-comparison-file delete-file)
          (some-> result :cache :local-iiif-file delete-file)
          (some-> result :cache :local-original-file delete-file)))

      (let [stats @stats
            num-remaining (- (:num-images stats)
                             (:num-processed stats))]
        (stderr (select-keys stats [:num-images :num-processed :num-errors :num-deviations]))
        (log :report result)
        (when-not (= num-remaining 0)
          (recur))))))

;; bootstrap

(defn parse-args
  [arg-list]
  (when-let [arg (first arg-list)]
    (cond
      (url? arg) [arg nil]
      (msid? arg) [nil arg]
      :else (throw (RuntimeException. "given argument is neither a url nor a manuscript id")))))

(defn -main
  [& args]
  (let [[iiif-url msid] (parse-args args)]
    (cond
      (some? iiif-url) (find-single-image iiif-url)
      (some? msid) (find-single-article msid)
      :else (find-all))

    ;; start processing images added by the find-* functions
    (run! image-processor (range num-processors))

    ;; polls the results channel and prints progress
    (report)

    ;; write the final set of stats to the report
    (log :report @stats)

    ;; clean up
    (async/close! results-chan)))
