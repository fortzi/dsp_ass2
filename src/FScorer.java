import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;

import java.io.*;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * Created by user1 on 06/06/2016.
 */
public class FScorer {
    public static final int THRESHOLD_TESTS_RESOLUTION = 15;
    public static final double TESTS_THRESHOLD_GAP = (double) 10 / THRESHOLD_TESTS_RESOLUTION;
    public static final String VERDICT_COUNTERS = "VERDICT_COUNTERS";

    public enum Verdict {
        NA,
        TRUE_POSITIVE,
        TRUE_NEGATIVE,
        FALSE_POSITIVE,
        FALSE_NEGATIVE
    }

    public static class FScorerMapper extends Mapper<Object, Text, WordPair, VerdictWritableArray> {
//        String[] pos = { "america natives" , "circumstances ordinary" , "compelled give" , "great republicans" , "age present" , "dick growled" , "fishes north" , "one such" , "retorted sharply" , "data such" , "equal right" , "father left" , "figures hand" , "five years" , "promoting society" , "already flames" , "called child" , "former methods" , "instead suppose" , "irritated looked" , "john st" , "more years" , "scotts sir" , "came peter" , "county each" , "cut through" , "even national" , "new yorki" , "reason without" , "see thats" , "away tucked" , "based claim" , "big grunted" , "both works" , "contra see" , "em told" , "field two" , "fourteenth under" , "latter two" , "nations united" , "need spend" , "phenomena such" , "produce treatment" , "respect sex" , "above lived" , "bowman et" , "davis sammy" , "driven water" , "hours less" , "making statement" , "map north" , "natural rate" , "nights spent" , "problem somehow" , "turbulent waters" , "atheist hes" , "being means" , "biography current" , "brings map" , "closely connected" , "common species" , "day next" , "exerted influence" , "fort taking" , "man observed" , "paucity relative" , "provincial shall" , "social term" , "street way" , "always hes" , "always talking" , "bank case" , "body consumed" , "boston narrows" , "devastating more" , "ecole louvre" , "effects term" , "even though" , "few years" , "four miles" , "further nothing" , "graceful those" , "investigations preliminary" , "male white" , "man young" , "medicine psychosomatic" , "observers political" , "once released" , "pontiff sovereign" , "table vii" , "thing writhes" , "architectural genius" , "brief ln" , "cancer gastric" , "cent per" , "combination single" , "come messenger" , "day man" , "day mg" , "decision rent" , "disadvantage potential" , "emphasized men" , "exact locate" , "find good" , "front passed" , "good view" , "learning means" , "major toxicities" , "out wipes" , "perhaps telling" , "reason shows" , "abundantly present" , "addition another" , "american philosophy" , "asymptotically uniformly" , "back settled" , "bank second" , "better much" , "biological control" , "colin general" , "consumption function" , "context indicates" , "crowd weeping" , "cultured person" , "dancing re" , "down line" , "explanation offer" , "final result" , "national socialist" , "navigated way" , "oracle within" , "others saw" , "ring waldeyers" , "six thousand" , "stable uniformly" , "za zavod"};
//        String[] neg = { "actors key" , "back going" , "intention primary" , "jacques monsieur" , "something think" , "appropriated congress" , "boy small" , "one such" , "came speak" , "economic relations" , "henry sir" , "never see" , "aircraft battery" , "bulb large" , "came minister" , "continues diary" , "difficulty raised" , "dorsal surface" , "home peter" , "limbs trunk" , "statement useful" , "competition refers" , "dated picture" , "duty legal" , "godfrey haller" , "joint routes" , "ll never" , "above data" , "above give" , "adequate survey" , "appended example" , "curve linear" , "external such" , "farmer small" , "followed party" , "friendly received" , "given statement" , "hardly seems" , "minutes three" , "notes one" , "remember seen" , "scene such" , "acts someone" , "aggregation kind" , "buck stove" , "cent per" , "context full" , "courtly love" , "east wind" , "factors undoubtedly" , "glasses rimmed" , "head under" , "infant premature" , "information up" , "jack uncle" , "lease signed" , "measurement negro" , "nearer one" , "pressure venous" , "qualities same" , "states united" , "tools useful" , "administrative costs" , "cut lets" , "defined now" , "door opened" , "four hundred" , "gas natural" , "geographical journal" , "leaves open" , "mg protein" , "one though" , "perhaps thought" , "preservation self" , "reinforcement used" , "announced document" , "appears upshot" , "approximately halfway" , "arms those" , "bacteria negative" , "before leaving" , "belongs law" , "biology comparative" , "both economic" , "cheung ming" , "chosen man" , "common risk" , "convinced sound" , "days two" , "during parliamentary" , "epithelium squamous" , "feet second" , "final takes" , "find something" , "include processes" , "leaving re" , "lived tribes" , "more one" , "paper put" , "prince wales" , "such times" , "addition causing" , "al et" , "authority weight" , "bernard george" , "change impulse" , "chapter next" , "chapter see" , "cocks shrill" , "colin powell" , "cyr et" , "deer north" , "difference key" , "different symbols" , "down passing" , "et roe" , "financial instability" , "first summer" , "followed river" , "footnote reads" , "form units" , "freely tears" , "goest thou" , "good public" , "here shall" , "hes thats" , "inch thirteen" , "integral number" , "laboratories use" , "literacy rates" , "loss weight" , "monyghams possible" , "more phrase" , "moved people" , "operating standard" , "probably send" , "problems special" , "savezni zavod" , "stepped ve" , "take things" };
        String[] pos = { "actors key", "alluding perhaps", "around went", "art romantic", "bed entire", "both two", "calendar newgate", "carlyles life", "clear made", "compelled give", "criminal matters", "exactly same", "great many", "great republicans", "happens sometimes", "king shall", "look out", "one ordinary", "romantic true", "septum usually", "age present", "attempted murder", "boys school", "browns speech", "chief justice", "dick growled", "divided state", "down winding", "each units", "earnings gross", "earnings mile", "earnings per", "fellow poor", "felt heart", "fishes north", "intention primary", "johns school", "joint stock", "little observation", "little shows", "more remote", "name whats", "new rifle", "one talks", "powers three", "quoted remark", "very well", "action second", "another example", "appropriated money", "boat whose", "boy small", "consider great", "cultures old", "data such", "enough knew", "equal right", "exchange system", "first january", "five years", "flowers yellowish", "formed part", "fund service", "gives little", "going upstairs", "gone ve", "late time", "makes very", "new york", "nitrate silver", "order standardise", "place take", "plan suggested", "public service", "service superannuation", "surest thing", "arrangement seemed", "baron de", "baron hirsch", "believe syllable", "bend shall", "blood groups", "bottom layer", "called child", "came speak", "case classic", "clearly see", "connects railroad", "continuum time", "cunningham writes", "desert heat", "distance mean", "effect traces", "figures think", "find hard", "general grant", "henry very", "instead suppose", "life long", "lines south", "next reported", "oakhurst received", "one unit", "oxide zinc", "places various", "received sentence", "saw sec", "saw stark", "scotts sir", "silent voice", "texts used", "aircraft battery", "bulb large", "came peter", "cent per", "chosen ve", "county entitled", "cut through", "difficulty raised", "feel warmth", "few patients", "few very", "god knows", "heard others", "henry yorki", "home peter", "inductance self", "industry now", "iron made", "iron wrought", "land mine", "limbs trunk", "men wait", "new yorki", "re smoking", "reason without", "see thats", "accidents such", "act those", "adult average", "although larger", "anything never", "areas two", "aside put", "asked girl", "asked jesus", "attempted quantify", "average spends", "away tucked", "big grunted", "books looking", "borromeo charles", "cases out", "characteristic defining", "cohesive strength", "come home", "company make", "competition refers", "completed having", "completed water", "contra see", "contribution main", "control want", "died march", "discover nurse", "election such", "ergibt es", "ergibt sich", "especially seemed", "field two", "fig see", "git right", "haller william", "henry james", "house see", "inclinable person", "incomes study", "joint routes", "latter two", "ll never", "man though", "miles north", "number operas", "out tiptoed", "owner told", "phenomena such", "plowing time", "produce treatment", "respect sex", "routes through", "spread story", "above data", "above lived", "adequate survey", "afraid became", "al el", "al et", "already done", "already two", "already ve", "appended example", "approach use", "art fogg", "art museum", "association traditional", "assumed electrodes", "black pedro", "bless god", "body discovered", "bottom right", "brand english", "brand rate", "calibrated pyrometer", "carry decided", "causes such", "class working", "composed presidium", "congress party", "contract price", "daniel fact", "davis sammy", "des du", "dorothea glanced", "dost thou", "driven water", "dry weight", "during time", "eight hours", "et fernald", "external such", "first headed", "first item", "free interval", "given statement", "hardly necessary", "having remember", "increased production", "kind traditional", "labor practice", "labor unfair", "larger states", "lastarria victorino", "liver substance", "lot saved", "majority overwhelming", "make man", "mention want", "mercantile nat", "natural rate", "notes one", "observations practical", "oral stage", "out rose", "person younger", "remember seen", "scene such", "situation such", "third world", "turbulent waters", "abounds older", "acid sequence", "acre tract", "act stamp", "administration planning", "africa loss", "aid hearing", "al et", "american barnums", "american museum", "american north", "american scandinavian", "americans anglo", "amount certain", "animal sei", "being means", "biography current", "black task", "blood venous", "both models", "brings map", "buck stove", "called mother", "captured shadow", "child guidance", "class dining", "common species", "complete following", "congress stamp", "continued section", "copies sufficient", "coup toppled", "course law", "courtly love", "de juntas", "de ofensiva", "different quite", "dining room", "district inspector", "doing mechanism", "drudgery sheer", "eastern united", "eleven one", "eleven year", "example first", "family history", "fort taking", "forward moved", "free means", "hand patients", "hat pushed", "immediate release", "include last", "information up", "inspector schools", "intervals minute", "introduction well", "know much", "learning term", "lets look", "man observed", "map out", "maxillary sinus", "measurement negro", "methods research", "mid winter", "moons pull", "nearer one", "nephrotoxicity risk", "new something", "outer world", "patient see", "personally remember", "pressure venous", "provincial treasurer", "put well", "qualities same", "really suppose", "results such", "rules system", "social term", "someone until", "strategy structure", "student suggested", "summing up", "tools useful", "above described", "abundant gas", "act congress", "addition bone", "always hes", "always talking", "authorized shall", "authors several", "bad results", "bank case", "believe children", "body consumed", "born children", "born out", "break shall", "building splendid", "capacity identify", "care centered", "case reminiscent", "cases require", "child place", "common occurrence", "consideration more", "critiques detailed", "de ile", "described under", "direct investigation", "drink go", "efficiency issue", "end here", "enough thats", "even though", "few years", "figure lowest", "financial times", "foot set", "four miles", "frequency reduce", "further known", "further nothing", "gas natural", "going marry", "good natured", "graceful those", "guerrilla war", "hour workweek", "implications psychoanalytic", "implications recent", "incident komagata", "included plan", "inhabitants white", "injections intravenous", "inner meaning", "interpretation psychoanalytic", "investigations suggest", "latter process", "looked one", "man young", "many persons", "marginal policies", "medicine psychosomatic", "mg protein", "ml protein", "new order", "new religions", "nobility polish", "nominated northern", "notes psychoanalytic", "obsessive see", "official took", "once released", "one though", "open possibility", "partial removal", "patriotic wise", "per volume", "perhaps thought", "pity those", "pontiff sovereign", "position stems", "preservation self", "priorities term", "purely terms", "result taken", "table vii", "thing writhes", "acid naphthylphthalamic", "acid oleic", "adolescent marijuana", "alexander teacher", "although bacteria", "amendment fifth", "announced document", "approximately halfway", "area mile", "arms those", "art god", "assistance financial", "attempts unsuccessful", "best instruction", "between halfway", "biology comparative", "body medicine", "both economic", "british committee", "burger king", "came knew", "cancer gastric", "catalogue record", "certeau michel", "change loss", "cheung ming", "cities two", "class voters", "come messenger", "come pay", "comedie humaine", "comedie la", "committee standards", "common factors", "common risk", "concluded patients", "conductivity detectors", "consists gel", "controlled study", "convinced sound", "crossed enemy", "day mg", "de michel", "decision rent", "democratic system", "dieu seigneur", "disadvantage potential", "discover hours", "dose range", "dotted line", "during parliamentary", "economical process", "educational opportunities", "eight feet", "energy self", "epithelium squamous", "evening spent", "factors such", "find something", "fix ll", "flexibility provides", "formerly prince", "forming habit", "fuck oh", "gated na", "going re", "good view", "great provides", "green sort", "hoped researchers", "idea market", "implicit psychology", "indication primary", "induction motors", "involved steps", "job thats", "know really", "leaving london", "leaving soon", "lets now", "lets out", "lived tribes", "major toxicities", "medium series", "miles per", "moment one", "more one", "one try", "ostensibly though", "paper put", "pd wagner", "percentage university", "pestilence poisoning", "pion press", "political roles", "practices trade", "predecessors unlike", "prince wales", "protein shock", "reading without", "really scheme", "resounding through", "semantics without", "shear thinning", "significance special", "such times", "systems those", "tried vain", "above air", "addition another", "again walked", "ago fourteen", "al assasat", "although many", "always embarrassed", "american philosophy", "ancient cities", "angels re", "another one", "answered phone", "appraisal economic", "arm twisting", "associations similar", "back go", "back settled", "began course", "begin really", "benefits social", "better much", "between key", "boston history", "bring ll", "bynum wf", "cent per", "cent smaller", "century th", "champion paris", "change impulse", "chapter next", "charitable see", "chelune et", "chess pieces", "civil structure", "claim such", "clarion cocks", "clearly context", "cocks shrill", "colin general", "complaints led", "confession way", "confirms observations", "consumption function", "consumption relates", "contact eye", "continuous model", "control natural", "correlation spatial", "couple times", "crowd weeping", "cut material", "cyr et", "dad shook", "dans pleure", "day intervals", "destruction habitat", "deviation standard", "diabetes mellitus", "diagonal elements", "difference key", "different symbols", "door false", "dramatic single", "east far", "edition nd", "editions various", "education higher", "employer shall", "equilibrium quasi", "establishment sound", "et roe", "even stop", "evening towards", "everywhere went", "examples give", "femoris lateralis", "femoris rectus", "followed river", "footnote reads", "form units", "fungal infections", "general younger", "given high", "goest thou", "government kenya", "having out", "hierarchy illustrated", "high profile", "highly injurious", "history literature", "history memorial", "house keep", "house shall", "human thousand", "improvements led", "indication one", "initial overcome", "japan trade", "jeremy mr", "kenntniss zur", "kg ml", "league saloon", "linda returned", "listened women", "ll probably", "ll see", "look shall", "loss weight", "malcolm points", "many people", "mild simple", "modos todos", "more phrase", "moved people", "much really", "national socialist", "navigated way", "negotiations trade", "new took", "october th", "one piece", "one year", "operating procedure", "operating standard", "oracle within", "ordered round", "out pointing", "output port", "over shes", "pay willing", "point view", "possible return", "probably send", "project whole", "purchase second", "several times", "six weeks", "skulls thousand", "stepped ve", "take things", "task ultimate", "techniques used", "through vainly", "took whole", "very works", "well works", "za zavod" };
        String[] neg = { "america natives", "boy old", "character pious", "child posthumous", "circumstances ordinary", "cook herbebt", "cor iv", "deeper portion", "function principal", "hands pressed", "henry king", "henry navarre", "interesting more", "life sterling", "lord tenterden", "made up", "meant one", "more still", "never saw", "never such", "occurs several", "ordinary under", "pill rob", "remained under", "two women", "back going", "bank stock", "browns john", "browns last", "bulletin farmers", "collection whole", "concentrated solutions", "condition conscious", "conditions resulting", "cried master", "experiments such", "flight took", "intermediate step", "jacques monsieur", "learning man", "name remembered", "now one", "one such", "portions selected", "pretty young", "retorted sharply", "simply waited", "something think", "appropriated congress", "celestial city", "effect solvent", "father left", "figures hand", "gave half", "gone through", "house one", "known proverb", "life think", "little makes", "little volume", "long strain", "many states", "promoting society", "significance wide", "sum up", "already flames", "amer econ", "amusing example", "better leave", "british dutch", "cage placed", "carolina north", "completion impending", "cover outer", "de francis", "de sales", "dignity gentle", "during hour", "economic relations", "even never", "famous leader", "feeling now", "former methods", "gave ideas", "general meet", "give something", "henry sir", "illness occur", "irritated looked", "john st", "life one", "ll understand", "love religion", "lower temperatures", "measurement social", "more years", "never see", "orchestra played", "page see", "production relations", "represent seem", "sir walter", "arose went", "author writes", "authorised person", "came minister", "came one", "came upon", "charlotte wrote", "continues diary", "county each", "dorsal surface", "even national", "great left", "hate having", "heard ve", "holt yorki", "later publications", "new reached", "next summer", "orleans reached", "peter st", "ready steamer", "report went", "respectfully submitted", "roughly speaking", "statement useful", "thought writer", "ago years", "air open", "aluminum co", "approach cold", "arab world", "asia command", "asia east", "assessment respect", "based claim", "big man", "both works", "broke john", "buon giorno", "burning fire", "chairman first", "charged complaint", "concept translate", "contributory negligence", "daraus ergibt", "dated picture", "doctrine nowhere", "early summer", "effect net", "em told", "emphasized importance", "events significance", "explained operation", "firmly resolved", "floor level", "fourteenth under", "girl russian", "godfrey haller", "governing regulations", "heading under", "journals periodicals", "nations united", "need spend", "never silent", "never sit", "never trouble", "pancreatic volume", "scenes show", "see under", "two windings", "ways works", "above give", "above statement", "again permit", "al bowman", "aside setting", "bank nat", "became people", "believe thou", "beuve tells", "big business", "bill hope", "bone joint", "bowman et", "brain increase", "cases murder", "cf schmidt", "chat fireside", "coming second", "compact provide", "controversy macarthur", "coordinates three", "curve linear", "curve strain", "cycle life", "depressive psychosis", "droit du", "dry mg", "du histoire", "equal shall", "exports value", "farmer small", "first party", "followed party", "following name", "forster writes", "friendly received", "glanced quickly", "hardly seems", "hours less", "imitation instinct", "involvement low", "jose victorino", "lines west", "ll make", "love poems", "making statement", "map north", "minutes ten", "minutes three", "new york", "nights spent", "pointed shall", "problem somehow", "purity spotless", "soviet understanding", "stage third", "those years", "acid amino", "acid information", "activity ph", "acts someone", "affair organized", "agency co", "aggregation kind", "ago years", "agreement exists", "agreement little", "akad wiener", "always room", "american review", "annual meeting", "another student", "areas surplus", "atheist hes", "attacked french", "back better", "better come", "black white", "brought suit", "cent per", "century figure", "cheshire lancashire", "child clinic", "child wilder", "chopped cup", "chopped pecans", "closely connected", "co ordination", "commander decided", "context full", "day next", "de nacional", "defense department", "delegates elected", "der wiener", "differences reflect", "each one", "east wind", "ed rev", "ego super", "elapsed years", "erythrocytes presence", "et rubinstein", "even societies", "example north", "exerted influence", "eyes softened", "factors undoubtedly", "fellow selling", "fellow such", "few quotations", "fumimaro konoye", "fumimaro prince", "glasses rimmed", "god willing", "government proclaimed", "gravitational moons", "having won", "head under", "history rome", "imply observations", "infant premature", "jack uncle", "know people", "last loan", "law year", "lease signed", "literature older", "local planning", "method stacking", "models used", "month per", "now one", "out reaching", "petrographic study", "primary task", "provincial shall", "question therefore", "range stove", "re worried", "reliability types", "revolution such", "river swanee", "sixty years", "states united", "stood watching", "street way", "such transfer", "such without", "ten years", "thats well", "told two", "two types", "administrative costs", "although democracy", "another one", "appears thus", "archaeologists suggested", "assumes strategy", "authors deal", "back leans", "basis optimism", "blank verse", "book covers", "boston narrows", "boy reached", "century first", "children recommended", "children young", "climb graceful", "coca erythroxylon", "compulsive obsessive", "contain papers", "control social", "controversial remains", "cut lets", "daniels vision", "days fifteen", "defined now", "democrats northern", "detailed more", "devastating more", "discuss here", "discuss two", "door opened", "du ecole", "during region", "ecole louvre", "effects term", "elements pairs", "enable plants", "end ll", "end up", "epoch rapping", "evidence tables", "few older", "fifteen within", "findings suggest", "four hundred", "gain net", "geographical journal", "george hadn", "god pity", "grow plants", "hear whenever", "ile java", "includes space", "inside set", "international vi", "investigations preliminary", "involvement pattern", "journal london", "kinetic studies", "komagata maru", "leaves open", "lets out", "love still", "male white", "material thing", "med microbiol", "missed ve", "never shall", "observers political", "per weight", "present production", "really scared", "red thing", "reinforcement used", "reinforcement vicarious", "repeat steps", "requests responds", "suggest those", "accepted gifts", "adolescent use", "allowance recommended", "anemia deficiency", "appeared lighted", "appears upshot", "art thou", "asked sheridan", "bacteria negative", "before leaving", "belongs law", "beyond new", "both sides", "boundary model", "brief ln", "bryans william", "bus lighted", "cent per", "cephalosporins generation", "children performed", "chosen man", "clothes thrown", "combination single", "come here", "concerned ostensibly", "conductivity thermal", "corp king", "crest iliac", "dadra nagar", "day man", "days two", "deficiency iron", "democratic political", "design want", "dietary recommended", "economic growth", "economic summit", "educational formal", "emphasized men", "enzymol methods", "exact locate", "explain help", "expressions following", "extraordinary group", "feel people", "feet per", "feet second", "final scene", "final takes", "find good", "fix up", "four hours", "front passed", "gastric patients", "give satisfaction", "go lets", "going nowhere", "governed process", "having returned", "heat shock", "hour per", "husband more", "ii nitrogen", "important three", "include processes", "independent manner", "induction speed", "inexpensive method", "islamic number", "jennings william", "later two", "learning means", "leaving re", "lexington mass", "lithium study", "look russian", "made promise", "make planning", "ming tai", "mother one", "new psychology", "new york", "nothing party", "now see", "number ranges", "out wipes", "over troubles", "palace westminster", "pattern response", "perhaps strangest", "perhaps telling", "reason shows", "role seems", "seemed simple", "show statistics", "sides thought", "sketches use", "sokhta tepe", "sound very", "steps various", "structural variables", "three twenty", "two years", "above hill", "abundantly present", "accomplished work", "add code", "addition causing", "admit hesitate", "agreed old", "agreement picot", "air space", "al et", "al risala", "alabama apparently", "alabama law", "although few", "always head", "amounted numbers", "angrily growled", "answered little", "approach innovative", "approaches many", "arm scenes", "ask ll", "ask now", "assumes thus", "assumptions day", "asymptotically uniformly", "authority weight", "awoke following", "back walked", "bank second", "bear try", "being material", "beitrage zur", "bernard george", "bernard shaw", "better those", "between distinction", "bilateral halves", "biological control", "brayton et", "caption reads", "care centers", "carried excavations", "carried fifth", "changing economy", "chapter see", "charitable critics", "cheaper th", "colin powell", "columbia university", "communication general", "communication patient", "community standards", "context indicates", "cook love", "copy personal", "corinth destruction", "corner east", "cortex parietal", "cultured person", "dancing re", "de todos", "deer north", "deer tailed", "der zur", "differential mode", "differential response", "dig more", "dish mm", "dislike style", "distinction qualitative", "down line", "down passing", "each house", "eds planning", "employer obtain", "end one", "environment trade", "exchanged salutations", "explanation offer", "far short", "fellow wonderful", "femoris vastus", "fifth final", "final result", "financial hypothesis", "financial instability", "first summer", "flowed tears", "fourteen months", "freely tears", "fungal threatening", "gone having", "handbook industrial", "hc ostgaard", "head shook", "hear ye", "held meeting", "help ll", "here shall", "hes thats", "higher level", "highly students", "hope word", "hour late", "idee que", "inch thirteen", "infinite vainly", "informed state", "integral number", "john took", "knew power", "korea republic", "laboratories use", "lake quite", "large similar", "later weeks", "level security", "lie roots", "literacy rates", "ll soon", "meeting one", "mellitus proc", "methods still", "methods such", "mon pleure", "monyghams possible", "more one", "more recent", "musky odor", "number wavelengths", "old woman", "organs power", "others saw", "out points", "passed six", "plentifully supplied", "point up", "power state", "presented two", "problems related", "problems special", "progresses weight", "prohibited shall", "proposals recent", "public revenues", "raised up", "richard white", "ring waldeyers", "rule year", "savezni zavod", "see today", "settle soon", "six thousand", "smallest unit", "social web", "stable uniformly", "statistiku zavod", "symbols used", "today visitors", "vi vol" };


        HashMap<String, Boolean> posPairs;
        HashMap<String, Boolean> negPairs;

        private String sortWords(String str) {
            String[] words = str.split(" ");
            boolean w1After = words[0].compareTo(words[1]) > 0;
            return words[w1After ? 1 : 0] + " " + words[w1After ? 0 : 1];
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            posPairs = new HashMap<String, Boolean>();
            negPairs = new HashMap<String, Boolean>();

            for (String str : pos)
                posPairs.put(sortWords(str), true);

            for (String str : neg)
                negPairs.put(sortWords(str), true);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] decadeCarCdr = keyValue[0].split(" +");

            int decade = Integer.parseInt(decadeCarCdr[0]);
            String car = decadeCarCdr[1];
            String cdr = decadeCarCdr[2];
            String pair = car + " " + cdr;
            double pmi = Double.parseDouble(keyValue[1]);

            VerdictWritableArray verdicts = new VerdictWritableArray(THRESHOLD_TESTS_RESOLUTION);
            boolean localVerdict;

            for (int i = 0; i < THRESHOLD_TESTS_RESOLUTION; i++) {
                localVerdict = pmi >= (i + 1) * TESTS_THRESHOLD_GAP;

                if (localVerdict && posPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.TRUE_POSITIVE);
                else if (localVerdict && negPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.FALSE_POSITIVE);
                else if (!localVerdict && posPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.FALSE_NEGATIVE);
                else if (!localVerdict && negPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.TRUE_NEGATIVE);
                else
                    verdicts.setVerdict(i, Verdict.NA);

                context.getCounter(VERDICT_COUNTERS, verdicts.getVerdict(i).name() + "_" + (i + 1)).increment(1);
            }
//            context.write(new WordPair(car, cdr, decade), verdicts);
        }
    }

    public static class FScorerReducer extends Reducer<WordPair, VerdictWritableArray, WordPair, VerdictWritableArray> {

        public static void print() throws IOException {

            File resultsFile;
            PrintWriter results;

            resultsFile= File.createTempFile("test-", ".txt");
            resultsFile.deleteOnExit();
            results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));

            results.printf("this is just a line for test 1");
            results.printf("this is just a line for test 2");
            results.printf("this is just a line for test 3");
            results.printf("this is just a line for test 4");

            results.flush();
            results.close();

            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            new S3Helper().putObject(S3Helper.Folders.LOGS, resultsFile);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        }

        protected void reduce(WordPair key, Iterable<VerdictWritableArray> values, Reducer<WordPair, VerdictWritableArray, WordPair, VerdictWritableArray>.Context context) throws IOException, InterruptedException {

            print();
        }


        protected void cleanup(Reducer<WordPair, VerdictWritableArray, WordPair, VerdictWritableArray>.Context context) throws IOException, InterruptedException {
            long tp, fp, fn;
            double precision, recall, F;
            File resultsFile;
            PrintWriter results;

            resultsFile= File.createTempFile("FScorer_", ".txt");
            resultsFile.deleteOnExit();
            results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));

            for (int i = 0; i < THRESHOLD_TESTS_RESOLUTION; i++) {
                tp = context.getCounter(VERDICT_COUNTERS, Verdict.TRUE_POSITIVE.name() + "_" + (i + 1)).getValue();
                // tn = context.getCounter(VERDICT_COUNTERS, Verdict.TRUE_NEGATIVE.name() + "_" + (i + 1)).getValue();
                fp = context.getCounter(VERDICT_COUNTERS, Verdict.FALSE_POSITIVE.name() + "_" + (i + 1)).getValue();
                fn = context.getCounter(VERDICT_COUNTERS, Verdict.FALSE_NEGATIVE.name() + "_" + (i + 1)).getValue();
                // na = context.getCounter(VERDICT_COUNTERS, Verdict.NA.name() + "_" + (i + 1)).getValue();

                precision = tp + fp != 0 ? (double) tp / (tp + fp) : 1;
                recall    = tp + fn != 0 ? (double) tp / (tp + fn) : 1;
                F = 2.0 * precision * recall / (precision + recall);

                results.printf("Threshold: %.2f, F: %f\n", (i + 1) * TESTS_THRESHOLD_GAP, F);
            }

            results.printf("cleanup\n");

            results.flush();
            results.close();

            new S3Helper().putObject(S3Helper.Folders.LOGS, resultsFile);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            System.out.println("please enter input and output paths");
            return;
        }

        Job fScorerJob = Job.getInstance(new Configuration(), "FScorer");
        fScorerJob.setJarByClass(FScorer.class);
        fScorerJob.setJobSetupCleanupNeeded(true);
        fScorerJob.setMapperClass(FScorer.FScorerMapper.class);
        fScorerJob.setReducerClass(FScorer.FScorerReducer.class);
        fScorerJob.setNumReduceTasks(1);
        fScorerJob.setOutputKeyClass(WordPair.class);
        fScorerJob.setOutputValueClass(VerdictWritableArray.class);
        FileInputFormat.addInputPath(fScorerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(fScorerJob, new Path(args[1]));

        fScorerJob.waitForCompletion(true);

        System.exit(0);
    }
}
