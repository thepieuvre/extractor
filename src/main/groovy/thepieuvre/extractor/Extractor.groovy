package thepieuvre.extractor

import com.cybozu.labs.langdetect.Detector
import com.cybozu.labs.langdetect.DetectorFactory
import com.gravity.goose.Configuration
import com.gravity.goose.Goose

import de.l3s.boilerpipe.extractors.ArticleExtractor

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

import java.net.URL

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j

import org.apache.log4j.PropertyConfigurator


@Log4j
class Extractor {

	private Configuration conf = new Configuration()
	private Goose goose
	private ArticleExtractor boilerpipe = ArticleExtractor.INSTANCE

	private def repushed = [:]

	private JedisPool pool

	private static def cli

  	static {
  		cli = new CliBuilder(
			usage: 'extractor [options] [url|redis]\n\turl: take an URL\n\tredis: redis mode', 
        	header: 'The Pieuvre - Extractor: Smart Article fetcher',
        	stopAtNonOption: false
    	)
		cli.h longOpt: 'help', 'print this message'
		cli.c longOpt: 'config', args:1, argName:'configPath', 'configuration file\'s path'
		cli._ longOpt: 'redis-host', args:1, argName:'redisHost', 'redis server\'s hostname'
		cli._ longOpt: 'redis-port', args:1, argName:'redisPort', 'redis server\'s port'
		cli._ longOpt: 'redis-url', args:1, argName:'redisUrl', 'redis server\'s url -- server.domain.com:456'
		cli._ longOpt: 'lang-profile', args:1, argName:'profilePath', required:true, 'Lang profile\'s path (mandatory)'
	}

	Extractor(File profileDirectory, String redisHost, int redisPort){
		conf.enableImageFetching = true
		conf.imagemagickIdentifyPath= '/usr/local/bin/identify'
		conf.imagemagickConvertPath = '/usr/local/bin/convert'
		conf.connectionTimeout = 10000
		conf.socketTimeout = 30000
		goose = new Goose(conf)
		DetectorFactory.loadProfile(profileDirectory)
		JedisPoolConfig config = new JedisPoolConfig();
		config.setTestOnBorrow(true);
		pool = new JedisPool(config, redisHost, redisPort, 180000)
	}

	def goose(String link) throws Exception {
		def result = [:]
		result.extractor = 'Goose'
		def gExt = goose.extractContent(link)
		result.fullText = gExt.cleanedArticleText()
		result.mainImage = gExt.topImage.getImageSrc()
		return result
	}

	def boilerpipe(String text) throws Exception {
		def result = [:]
		result.extractor = 'Boilerpipe'
		result.fullText = boilerpipe.getText(text)
		return result
	}	

	def extracting(String link) throws Exception {
		def result = [:]
		try {
			result = goose(link)
		} catch (Exception e) {
			 log.error "${new Date()} - Exception from goose ${e.getMessage()}", e
		}
		if (result.fullText == null || result.fullText.isEmpty()) {
			def boilerpipe = boilerpipe(link)
			result.fullText = boilerpipe.fullText
			result.extractor = boilerpipe.extractor
		}
		return result
	}

	def guessLang(String text) {
		try {
			Detector detector = DetectorFactory.create()
			detector.append(text)

			return detector.detect()
		} catch (com.cybozu.labs.langdetect.LangDetectException e) {
			log.debug "No text for guessing"
			return 'no-lang'
		}
	}

	def redisMode() {
		log.info 'Starting listenning to the queue:extractor'
		while (true) {
			def task 
			def decoded
			Jedis jedis
			try {
				jedis = pool.getResource()
				task = jedis.blpop(31415, 'queue:extractor')
				if (task) {
					decoded = new JsonSlurper().parseText(task[1])
					log.info "${new Date()} - Extracting content for $decoded.id / $decoded.link"
					def extracted = extracting(decoded.link)
					def guessLang = (extracted?.fullText)?guessLang(extracted.fullText):guessLang(decoded.raw)
					def builder = new groovy.json.JsonBuilder()
					def root = builder.content {
						id decoded.id
						link decoded.link
						extractor extracted.extractor
						mainImage extracted.mainImage
						fullText extracted.fullText
						lang guessLang
					}
					jedis.rpush("queue:article", builder.toString())
					log.info "${new Date()} - Extracted and pushed to the queue:article: $decoded.id / $guessLang"
				} else {
					continue
				}
			} catch (de.l3s.boilerpipe.BoilerpipeProcessingException e) {
				if (task && decoded) {
					if (repushed[decoded.link]) {
						log.warn "${new Date()} - No content ${e.getMessage()} -- already repushed / remove it"
						repushed.remove(decoded.link)
					} else {
						log.warn "${new Date()} - No content ${e.getMessage()} -- repushed to the queue"
						repushed[decoded.link] = new Date()
						jedis.rpush("queue:extractor", task[1])
					}
				} else {
					log.info "${new Date()} - No content ${e.getMessage()}"
				}
			} catch (Exception e) {
				log.error "${new Date()} - Problem with ${decoded}", e
			}  finally {
				pool.returnResource(jedis)
			}
		}
	}

	void finalize() throws Throwable {
		pool.destroy()
	}

	private static Map parsingCli(String [] args) {
		def opts = cli.parse(args)

		if (opts.h) {
			cli.usage()
			System.exit(0)
		}

		if (opts.arguments().size() != 1) {
			System.err.&println 'Not enought arguments'
			cli.usage()
			System.exit(1)
		}

		def parsed = [:]

		parsed.profilePath =  opts.'lang-profile'
		String arg = opts.arguments()[0]
		parsed.redisMode = (arg == 'redis')
		if (! parsed.redisMode) {
			parsed.url = arg
		}

		parsed.redisHost = 'localhost'
		parsed.redisPort = 6379

		if (opts.'redis-url') {
			URI uri = new URI(opts.'redis-url')
			parsed.redisHost = uri.getHost()
			parsed.redisPort = uri.getPort()
		}

		if (opts.'redis-host') {
			parsed.redisHost = opts.'redis-host'
		}

		if (opts.'redis-port') {
			parsed.redisPort = opts.'redis-port' as int
		}

		if (opts.c) {
			def config = new ConfigSlurper().parse(new File(opts.c).toURL())
			PropertyConfigurator.configure(config.toProperties())
		}

		return parsed
	}

	static void main(String [] args) {

		def params = parsingCli(args)

		Extractor extractor = new Extractor(
			new File(params.profilePath),
			params.redisHost,
			params.redisPort
		)

		if (params.redisMode) {
			extractor.redisMode()
		} else {
			println "Starting extractor"
			try {
				def goose = extractor.goose(params.url)
				def boilerpipe = extractor.boilerpipe(goose.rawHtml)
				def lang = extractor.guessLang((goose.fullText)?:boilerpipe.fullText)

				println goose
				println '--------------------'
				println boilerpipe
				println '--------------------'
				println lang
			} catch(e) {
				System.err.&println "${new Date()} - Exception below:"
				e.printStackTrace()
			}
		}

	}
}