package main

import (
	"bufio"
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"io"
	"os"
	"reflect"
	"time"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*splitReader)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*logFn)(nil)).Elem())
}

// https://beam.apache.org/documentation/programming-guide/#sdf-basics
func main() {
	ctx := context.Background()

	fn, err := getDummyFile()
	if err != nil {
		log.Fatalln(ctx, err)
	}

	filesystem.ValidateScheme(fn)

	pipeline := buildSplitFileReaderPipeline(ctx, fn)

	// obviously splitting doesn't get parallelised by workers with local execution
	// this example is also really quite trivial. You might want to do such a thing
	// with massive files.
	start := time.Now()
	log.Info(ctx, start)
	if err := beamx.Run(ctx, pipeline); err != nil {
		log.Fatalln(ctx, err)
	}
	log.Info(ctx, time.Now())
	log.Info(ctx, time.Since(start))
}

type splitReader struct{}

// CreateInitialRestriction creates an initial workload restriction that encompasses the whole file.
func (fn *splitReader) CreateInitialRestriction(filename string) offsetrange.Restriction {
	restriction := offsetrange.Restriction{End: getFileLength(context.Background(), filename)}
	log.Debugf(context.Background(), "splitReader CreateInitialRestriction: %v", restriction)
	return restriction
}

func (fn *splitReader) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// SplitRestriction splits each file restriction into blocks based on a custom line restriction
func (fn *splitReader) SplitRestriction(fileName string, rest offsetrange.Restriction) []offsetrange.Restriction {
	fs, err := filesystem.New(context.Background(), fileName)
	if err != nil {
		log.Fatalf(context.Background(), "splitReader SplitRestriction: %v", err)
	}
	defer fs.Close()

	fd, err := fs.OpenRead(context.Background(), fileName)
	if err != nil {
		log.Fatalf(context.Background(), "splitReader SplitRestriction: %v", err)
	}
	defer fd.Close()

	r := bufio.NewReader(fd)

	// record the starting byte loc
	offset := rest.Start
	restrictions := make([]offsetrange.Restriction, 0)

	// scan the input
	s := bufio.NewScanner(r)
	for s.Scan() {
		// locate the next /n
		// record the len of bytes in scan
		size := len(s.Bytes())
		newOffset := offset + int64(size) + 1 // +1 for inclusive index

		// output the restriction
		next := offsetrange.Restriction{
			Start: offset,
			End:   newOffset,
		}
		log.Debugf(context.Background(), "splitReader SplitRestriction: %v", next)
		restrictions = append(restrictions, next)
		offset = newOffset
	}

	return restrictions
}

// RestrictionSize returns the size as the difference between the restriction's
// start and end.
func (fn *splitReader) RestrictionSize(_ string, rest offsetrange.Restriction) float64 {
	size := rest.Size()
	log.Debugf(context.Background(), "splitReader RestrictionSize: %v -> %v", rest, size)
	return size
}

func (fn *splitReader) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, filename string, emit func([]byte)) error {
	log.Debugf(ctx, "splitReader ProcessElement: Tracker = %v", rt)

	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	start := rt.GetRestriction().(offsetrange.Restriction).Start
	end := rt.GetRestriction().(offsetrange.Restriction).End
	width := end - start
	rd := io.NewSectionReader(fd, start, width)
	// Claim each line until we claim a line outside the restriction.
	for rt.TryClaim(start) {

		log.Debugf(context.Background(), "splitReader ProcessElement: claiming from %v", start)
		b := make([]byte, width)
		size, err := rd.Read(b)
		if err == io.EOF {
			if size != 0 {
				emit(b)
			}

			// Finish claiming restriction before breaking to avoid errors.
			end := rt.GetRestriction().(offsetrange.Restriction).End
			log.Debugf(context.Background(), "splitReader ProcessElement: claiming to %v", end)
			rt.TryClaim(end)
			break
		}
		if err != nil {
			return err
		}

		emit(b)
		start += int64(size)
		log.Debugf(context.Background(), "splitReader ProcessElement: moving offset to %v", start)
	}

	return nil
}

// logFn is a DoFn to log our split output.
type logFn struct{}

// ProcessElement logs each element it receives.
func (fn *logFn) ProcessElement(ctx context.Context, in []byte) {
	log.Infof(ctx, "splitReader Output:\n%s", in)
}

// FinishBundle waits a bit so the job server finishes receiving logs.
func (fn *logFn) FinishBundle() {
	time.Sleep(2 * time.Second)
}

func buildSplitFileReaderPipeline(ctx context.Context, file string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	log.Info(ctx, "pipeline.Add: beam.Create")
	files := beam.Create(s, file)

	log.Info(ctx, "pipeline.Add: files.splitReader")
	lines := beam.ParDo(s, &splitReader{}, files)

	log.Info(ctx, "pipeline.Add: lines.log")
	beam.ParDo0(s, &logFn{}, lines)

	return p
}

func getDummyFile() (string, error) {
	f, err := os.Create("dummy.text")
	if err != nil {
		return "", err
	}

	defer f.Close()

	if _, err := f.Write(
		[]byte(bigText)); err != nil {
		return "", err
	}

	return f.Name(), nil
}

func getFileLength(ctx context.Context, filename string) int64 {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalln(ctx, err)
	}

	fi, err := f.Stat()
	if err != nil {
		log.Fatalln(ctx, err)
	}

	return fi.Size()
}

var bigText = `Lorem ipsum dolor sit amet.
Eos commodi dolores ea omnis dolores sed unde nihil non nostrum officiis quo facilis animi et molestiae voluptatibus eos voluptatibus internos.
Est amet dolores ut magni galisum est molestiae consequatur et facilis harum et atque laborum et amet exercitationem.
Est quod officiis 33 numquam porro non soluta nisi et voluptatum corporis non ratione nobis in quibusdam quas.
Aut nemo galisum et soluta enim ad eligendi quas ut laudantium sequi in totam doloremque est itaque sapiente.
Qui commodi exercitationem in quisquam necessitatibus sit praesentium ipsa vel dolor reiciendis ea esse eius.
Et esse cumque in excepturi perspiciatis ut molestias dolorem?
Qui fuga reprehenderit et quas omnis sit ipsam soluta et rerum expedita rem dolor quas non numquam maiores.
Ex deserunt galisum cum tenetur provident sed nemo pariatur rem ratione molestias ut quos illo sit repellat repellat.
Sed quis enim 33 repellendus dolorum sit saepe amet et minima modi non explicabo harum ut voluptatem esse.
Vel alias dolor ut laudantium officia At enim aperiam aut impedit animi aut quis quis qui quidem dicta.
Non laudantium iusto ea voluptatibus praesentium a deserunt exercitationem.
Eum cumque quae qui nostrum fuga qui cupiditate optio.
Cum deleniti fuga sit nihil iure et dignissimos facere.
Non deleniti corporis At dolor enim ut sapiente nostrum et soluta culpa eum adipisci ipsam non cumque consequatur in dolores impedit.
Qui assumenda soluta qui vero doloremque eum ipsam ducimus aut illo quasi ut obcaecati natus eum voluptate alias.
Et similique aliquam rem quia galisum  facere dolores.
Et perspiciatis velit quo expedita quos et molestiae culpa.
Ut molestias autem sed voluptas aperiam ex odio tenetur quo voluptatum aperiam id aspernatur totam.
33 labore molestias et mollitia quia aut nostrum odit et quia quod.
Ex praesentium libero sit tempore quia et voluptatum deleniti aut quam voluptatem non blanditiis assumenda in nemo voluptate.
Aut repellat esse id laboriosam dolorem non natus magni.
Qui voluptatem necessitatibus 33 velit nemo et quidem corrupti quo asperiores repudiandae id voluptatem dolores non impedit voluptas.
Id voluptas ipsam ex assumenda voluptatem et aliquam cupiditate non internos mollitia qui repudiandae voluptas et nobis dicta est incidunt doloribus.
Quo deleniti internos aut galisum recusandae eos error consequuntur eos voluptatem ipsum id veniam autem nam architecto voluptas et incidunt corrupti.
Sit dignissimos distinctio ab quisquam aliquid vel asperiores dolores aut atque dolorem non iure quas est veniam quod.
Eum dolores voluptatem ut sequi deleniti aut fugit deleniti sed ipsam commodi.
Cum omnis recusandae aut iusto perferendis qui dolorem expedita non voluptas exercitationem et odio reiciendis eum sunt reprehenderit quo consequuntur doloremque.
Est delectus consectetur et aliquam delectus ut odio rerum et odit debitis?
Et excepturi consequatur non quaerat totam qui adipisci voluptatum aut illo beatae.
Eos nostrum dolorem ea rerum reiciendis sed facilis nemo.
Et nihil obcaecati est voluptates enim quo dolor quaerat ut delectus neque et nihil animi.
Ea corporis officia est ipsum eligendi est assumenda illo est laudantium animi ex consequatur ratione qui necessitatibus repellendus.
Aut illo nulla est reprehenderit exercitationem id aspernatur dolorem cum illo animi est consequatur consequatur sed eligendi corrupti.
Non optio itaque eum eius nemo qui rerum omnis sit atque commodi ex porro ullam.
Est quia temporibus aut asperiores consequatur qui odio neque.
Hic repellendus optio nam voluptates iste ut sunt doloribus sit aliquid aliquam! Non voluptates eius eum delectus illo ut earum nostrum.
Ut sapiente molestiae qui voluptas enim sed voluptatem suscipit eum numquam nobis! Aut assumenda velit qui sunt aliquid eum dolorum asperiores est alias voluptatem sit obcaecati voluptas.
Id culpa nisi ab rerum quia ut distinctio quam et aliquam explicabo hic voluptates nostrum nam earum animi.
Id inventore perferendis est quia architecto et corporis beatae non ipsam galisum.
Vel ducimus sunt aut molestias maiores est saepe vitae a ipsum obcaecati et dolor facere?
Qui vero Quis in necessitatibus excepturi et consequuntur maiores qui minima quasi ut nihil eaque est molestiae tempore non eligendi modi.
Est dignissimos exercitationem eum laboriosam iusto ut deserunt quia qui ullam quidem.
Qui maxime voluptatem vel aspernatur nesciunt id recusandae cupiditate ut sunt voluptates et dolorem neque nam ipsa Quis.
Et facilis numquam et architecto voluptatem aut nobis labore in autem nesciunt ut illo quia aut rerum voluptatem et dolores excepturi.
Ut perferendis rerum et nesciunt internos aut doloremque accusamus quo numquam galisum.
At explicabo laudantium aut voluptatum nemo et debitis quas ea atque ipsam ut veritatis voluptatibus.
Eum odio beatae eum quibusdam harum hic quibusdam mollitia et sapiente fuga ad quis asperiores et totam sunt non perspiciatis minima?
Est itaque voluptatum ab sapiente  est molestiae quia et expedita voluptatum et eaque unde! Consequatur sequi ad ullam repellendus ut voluptates rerum sit facilis corporis ad exercitationem cupiditate.
Vel obcaecati nisi ut illum natus et repellendus culpa ea aperiam distinctio qui distinctio internos et porro voluptates eum natus accusantium.
Et assumenda iusto sit quis voluptatum non quis nostrum eum galisum dolores aut tenetur ipsa nam commodi alias?
Rem voluptas vitae est molestias corporis eos quis sunt.
Et neque officia At necessitatibus internos qui galisum fugiat?
33 rerum temporibus hic similique odit sit tempora omnis ea tempora quia et facilis quos quo distinctio enim.
Quo nihil nobis et laudantium eius sit quae molestiae  eaque consequatur sit corporis fugiat ut voluptas ipsum sit eligendi consequuntur.
In quia voluptatibus a possimus rerum aut blanditiis quia qui possimus earum aut fugiat delectus id amet consequatur.
Et velit animi rem dolor fugit id suscipit consequuntur.
Nam error mollitia est blanditiis harum ex nisi internos est quasi error eos enim iusto aut temporibus veniam qui velit internos.
Et iure aspernatur in quod voluptatem et reprehenderit dolorem At beatae odio ea itaque cupiditate id enim beatae ea omnis quos.
Ut dolorem voluptas sit dolores magnam et dolorum unde aut numquam laborum.
Et dolor ipsa sit laudantium omnis et quos voluptas et quia debitis et dolor maiores.
Qui modi exercitationem et eveniet distinctio est consequatur laborum qui voluptatem dolore At dolores maxime et accusantium velit.
At corrupti dolor sit cupiditate sequi est omnis excepturi et consequuntur iusto et libero quisquam ut temporibus voluptatem?
Qui reprehenderit dignissimos est esse quia eum ullam corrupti! Et iste illum ad quos odio ad quod illum est fugit ipsa ea maxime officia At voluptatem sint.
Eos architecto porro rem esse dolorum aut sequi possimus ut molestiae quod et nisi earum qui  quidem.
Est quia praesentium id quam nemo et neque voluptas ut quas galisum.
Vel omnis sapiente aut voluptatem dolor est voluptatibus harum sed quidem consequatur.
Sed voluptas Quis eum accusantium omnis et magni ipsa et assumenda commodi et labore molestiae id magnam omnis.
Non reprehenderit quam est aperiam illo ea eligendi molestiae aut voluptatem itaque qui veniam dolor.
Eum tenetur perferendis sed iste tempore et minus mollitia eum molestias odit.
Sed galisum voluptatem aut accusantium dolores ut aspernatur architecto eos dolorum dolore.
Aut explicabo esse ea voluptatem unde aut eius sint vel consequatur inventore.
Quo obcaecati quibusdam ex molestias odit qui aspernatur beatae id illum eveniet qui culpa accusantium.
Ea repellendus recusandae est dolores voluptatibus ut pariatur ipsam quo soluta itaque et culpa laudantium aut nihil iste et doloribus iure.
Aut voluptatem provident et cumque aperiam ea iure voluptates qui nihil ipsa sit optio velit ut vitae doloribus aut quod quia.
Quo eaque earum est cupiditate perferendis ut voluptas molestiae qui sequi corporis eum explicabo asperiores.
Sed nemo reprehenderit et molestiae temporibus qui vitae rerum id quos quis aut dignissimos voluptates.
Nam magni impedit aut officia eaque est harum internos est iure voluptatem.
Aut provident modi sit eaque pariatur et voluptatem facere qui tempora expedita est distinctio totam ab natus omnis sit dolore quia.
Ea modi ipsa id quos ipsa id vitae amet aut unde sunt et cupiditate praesentium aut ipsam sequi et facilis accusantium.
Cum voluptatem sint aut voluptas amet et aliquam rerum.
Ut voluptatem maxime in consequatur debitis in maiores amet qui doloribus eligendi ad dolores esse in provident voluptatem.
Est dolores quaerat qui fuga fugiat in explicabo suscipit a aperiam rerum ex dolor eligendi ea consequatur blanditiis non libero dolorum.
Est quia facere aut esse dicta non alias eligendi rem vero aliquid ab quasi molestiae  tempore deleniti ut velit asperiores.
Non atque quam non temporibus  sed atque perspiciatis sit galisum galisum ab laudantium architecto.
Cum molestiae fugit ut deserunt cumque ad exercitationem iste.
Sit iste libero quo suscipit error id dolorum officia est magnam praesentium.
Id nobis optio  fuga enim ut consequatur unde et odit excepturi.
Aut porro fugiat aut internos esse est dolorem aspernatur.
Hic magnam harum et voluptas quos est voluptas recusandae! Id quae cupiditate qui reiciendis iste non doloribus rerum ut rerum doloribus est perspiciatis voluptatibus non  eligendi cum dolorum exercitationem.
Officia galisum qui sequi cumque ab aliquam quos?
Qui autem culpa non illo harum sed voluptas labore.
Non galisum quod ut quidem beatae est consectetur perspiciatis.
Eum ducimus aliquam sit alias sunt aut maiores ipsam ad omnis minima est similique consequatur.
Non praesentium perspiciatis hic omnis sapiente et sunt voluptatem aut iure veniam et aspernatur velit cum asperiores quam et cumque blanditiis! Et iure nihil qui galisum autem non deleniti porro.
Ut natus aspernatur 33 beatae quisquam et consequatur dolor qui officia tempore sit sunt ullam vel voluptas autem id voluptatibus voluptatum.
Et animi dicta vel deleniti architecto et dolorem consequatur aut consequatur autem ex atque molestiae.
Quod deleniti in dolore sapiente non dolores dicta aut voluptates harum nam harum odit.
Aut quasi aliquam et ipsa neque non quia distinctio ut error consequatur.
Vel dolorum facilis quo quod totam sed  nisi ut nihil enim sit magnam dolores et suscipit dolorum.
Et harum voluptatem et eveniet blanditiis qui voluptas nesciunt 33 iure quibusdam ab doloremque magni ab quis corporis aut rerum Quis.
Quo beatae consequatur aut blanditiis harum ex ipsam magnam ut molestiae possimus et soluta unde et mollitia sapiente! Ut fuga natus et suscipit vero et recusandae Quis a alias provident.
In omnis voluptate rem nihil nostrum aut dolorem omnis sed voluptates quia sit vitae cumque ut ducimus consectetur est autem aliquam! Eum dolor omnis sed pariatur voluptas ea modi dignissimos et enim laudantium sed internos voluptatum eum consequatur eaque! Et distinctio perspiciatis ad iure explicabo est tempore illum aut autem aperiam.
At nihil veritatis aut eveniet molestias et ratione ipsam ut autem rerum in voluptate obcaecati?
Qui recusandae fugiat in ipsa quaerat aut tempore ullam et numquam dolorem qui inventore nobis eum ipsam eius! Id enim quos est culpa voluptatem non quam omnis.
Ea dolorem vero aut accusamus optio qui minus libero sit distinctio sunt et internos modi et sint velit vel dolores illum.
Vel eius itaque aut voluptatem dignissimos et illo fugit?
Ut similique laborum At necessitatibus natus et porro nihil quo dolorem quidem.
Sit nulla iure At suscipit consequatur quo placeat rerum rem aperiam optio eum dolorum tempore et voluptatem atque in reprehenderit dolores.
Quo harum optio eum eligendi enim sit quae maiores.
Sit voluptatem temporibus quo nihil aperiam rem officiis veniam ea illo obcaecati.
Est recusandae deserunt eum provident quis et aspernatur sunt ut numquam facilis ea consequuntur voluptas.
In dicta mollitia nam officiis internos est consectetur officiis et porro natus est nisi molestiae ut suscipit autem aut quidem iusto! Id obcaecati rerum ut delectus obcaecati et perferendis officia sit nesciunt debitis aut totam corrupti.
In galisum molestiae et ducimus mollitia non odit officia non error amet ut Quis velit aut omnis quia.
Qui libero sint est voluptas laudantium qui aperiam nobis ut delectus facere rem sapiente corrupti ut dicta reiciendis.
A quaerat excepturi aut distinctio quia aut nihil minima.
Sit blanditiis dolor aut inventore similique quo odit maiores id molestias mollitia.
Et neque officiis ab error consequuntur et voluptatem provident sed minima quos.
Ab tempora cupiditate  suscipit quos aut omnis ipsum.
Ex facilis repellat At fugiat officiis sed vero rerum ea ipsa beatae nam consequatur quae sed neque dolore sed culpa accusamus.
Qui mollitia consequatur ut nobis aperiam eos praesentium sunt non culpa veritatis qui culpa eligendi eum fugiat quia vel omnis ullam.
Et galisum galisum aut fugiat veniam ut galisum voluptate.
At sint amet eos numquam doloremque qui expedita accusantium.
Et quia tenetur aut cumque consequatur est voluptatem quia eum voluptatum sunt.
Qui voluptas dolor est odio natus eos officiis voluptate est repellendus velit ea Quis neque cum autem quod! Ea quia vero qui veniam doloribus ut quia dignissimos ut voluptas iure eum laudantium natus aut voluptatem totam et error consequuntur.
Ab rerum enim id voluptatem magni hic omnis eveniet.
Sit molestiae doloremque  unde quos eum cumque natus?
Et unde deserunt et perferendis iure 33 quia porro nam molestiae laboriosam.
Rem cupiditate vero a omnis officia sit commodi velit qui molestiae quisquam et ratione nulla ut voluptas modi et ratione iste?
Quo unde ratione et dolorem optio est eveniet tenetur aut atque facere qui animi modi et mollitia perferendis sit laboriosam rerum! Et quas laboriosam aut animi assumenda ab  voluptatem.
Ut omnis nostrum ex dolorum libero est quia officiis sit soluta veritatis eos voluptatem quibusdam ut velit minima qui quibusdam fugit.
Hic aperiam incidunt a asperiores ipsum vel dolor unde?
Et earum nobis 33 autem aperiam ut nisi reiciendis ex sapiente dolor.
Ut rerum placeat et suscipit amet ut sint consectetur ut facilis ullam aut consequatur assumenda aut nulla magni! Qui Quis nesciunt et sint libero eos esse voluptatem in nisi impedit ad pariatur cupiditate.
Id eius officiis At delectus architecto non tenetur quia et officiis galisum?
Vel cupiditate omnis et fugit quam qui optio voluptas aut impedit provident sit architecto itaque cum tenetur eius est culpa omnis.
Et inventore harum At animi saepe nam architecto suscipit sed neque repellendus?
Et pariatur suscipit a sint sint eum repellat commodi At obcaecati obcaecati.
Est aperiam aliquam aut magnam error  voluptatem ipsum.
Est autem consectetur et esse accusantium sed tempore eaque eum Quis totam qui reiciendis minima ut soluta eligendi.
Ut fugit ullam a voluptates omnis aut similique alias qui accusantium quod id amet amet qui ratione vitae qui maiores quia.
Ut nulla rerum ab nesciunt explicabo a aliquid voluptatem aut repudiandae obcaecati et omnis quaerat ab voluptatum rerum.
Vel sint ratione sed velit modi est voluptatem distinctio et voluptas exercitationem in facere provident eos nesciunt dolore hic voluptatum fugit.
Cum voluptas unde sed nisi velit qui necessitatibus nobis non voluptates voluptates ex possimus velit.
Qui quis eius aut iusto voluptas id ipsam itaque et quis cupiditate aut galisum aspernatur qui nisi sapiente.
Qui   33 ducimus mollitia ut odio odio non rerum adipisci id molestiae quibusdam ea nihil facilis vel accusantium distinctio.
Id quia dolor 33 velit illo et quia earum ad autem repellat rem placeat quisquam.
Quo ipsum expedita et cumque facilis et illo enim.
Id dolores totam ut ipsam explicabo et reiciendis labore.
Est cumque voluptatibus ut maiores explicabo et incidunt illo ut sunt quia aut consectetur minima.
Non distinctio nobis  mollitia totam et ducimus ducimus est repellat rerum sit deserunt magni ea iusto perspiciatis?
Sed magni temporibus vel nemo consequuntur et possimus nihil.
Est repudiandae asperiores et asperiores maxime ea voluptatem eius non suscipit internos! Ut veritatis accusamus sit quibusdam internos quo natus itaque qui nobis consequatur ea dignissimos eveniet qui laudantium praesentium eos exercitationem veniam.
Id voluptatibus odit qui eius alias quo voluptatem sapiente At amet reiciendis et dolores deserunt.
33 animi laboriosam ex voluptas magni ut ipsa perspiciatis At tempora veniam.
Hic adipisci sint et similique porro ea natus internos.
Cum quidem odio id provident ipsam non odio perspiciatis.
Qui accusantium eaque ea tempora quia ut labore eius est molestias corrupti ut doloribus consequatur sit amet mollitia.
Ut deserunt saepe et suscipit assumenda ut tempora recusandae sit eveniet enim.
Est necessitatibus architecto cum nihil fuga ad soluta nobis est reiciendis magni.
Sit animi cupiditate ea dolorum natus rem sapiente nihil sed molestias perspiciatis ut modi galisum.
Sit praesentium quia ea repudiandae autem eos omnis fugiat est error earum et sunt deleniti aut autem nesciunt.
Eum quod voluptatem qui fugiat molestiae a obcaecati quam sed rerum nesciunt et recusandae vitae.
Sit laudantium maiores id officiis ullam non ipsam perferendis vel omnis distinctio rem excepturi facilis vel reiciendis eaque non reiciendis soluta.
Et porro eius et dolore voluptas et quasi repellat At odio dolorem.
Qui voluptas voluptatem in earum velit rem laboriosam suscipit cum ullam distinctio sit provident voluptatem non magnam omnis qui quibusdam reiciendis.
Aut velit rerum id nulla facere est voluptas sint et voluptatem autem et praesentium internos ex saepe animi hic deleniti consequatur.
Sed expedita dolor et sapiente animi ea beatae animi.
Eum atque velit id repudiandae voluptatem qui architecto eveniet ad quisquam eveniet aut consectetur sapiente et pariatur quos a dolorem possimus.
Sed minima enim in quas facere aut explicabo deleniti aut sunt eveniet.
Id iusto temporibus 33 numquam recusandae eos fuga sint non impedit distinctio non consequatur numquam hic dicta omnis ea voluptatem dignissimos.
Est nostrum voluptas eum dolorem excepturi sit rerum laborum.
Et voluptatem odio aut eveniet architecto sit alias corporis.
Non asperiores neque vel neque dolores et dolorum.
Sed iure itaque est dignissimos illum et aliquid neque hic error quos.
Sit consequatur sint ad quam excepturi ex consequatur ducimus et repellat dolores.
Cum architecto amet rem maiores dolorum ea maiores eius.
Ea neque aliquid aut praesentium voluptatem aut ullam tempora aut eligendi veniam eum rerum voluptas.
Et dolor modi non quia rerum nam accusamus quae.
Aut adipisci cumque sit libero sunt qui culpa internos 33 rerum delectus quo deleniti iusto! Sit quisquam obcaecati et unde maiores aut perspiciatis atque.
Vel omnis dolorum qui voluptatibus odit qui porro quia ad suscipit cupiditate et nisi laboriosam sit fuga natus.
Sed porro consequatur sit earum magni in tenetur facilis  commodi quibusdam ab dolorem quam.
Ut vero architecto ut animi explicabo et dolor quae! Eum officia quisquam a deleniti ipsa qui perspiciatis libero aut suscipit perferendis aut excepturi provident in omnis velit sit  accusantium.
Ab voluptatem sint vel sunt dolores a cupiditate maiores?
Sit Quis consectetur ea adipisci voluptas aut beatae voluptatem.
Sed dolorum ipsum qui eaque dolores qui corporis voluptatum.
Est amet quae eum quae omnis aut galisum voluptatem ad mollitia dolorem qui nisi molestiae et quia architecto ex dolorem sapiente.
Ut voluptatem eveniet vel neque Quis et impedit repellat et eius fugiat qui galisum  ea officia exercitationem quo totam voluptatem.
Sit minima nemo sed  magnam ut ipsam eveniet quo corrupti facilis id saepe explicabo.
Ab dolorum voluptas qui quas perspiciatis sit minima beatae et mollitia dolorem?
Et blanditiis doloremque qui exercitationem veniam et corrupti asperiores vel mollitia dolor.
Sit rerum dolorem aut doloremque laudantium et nemo aliquid sit consequatur porro et reprehenderit voluptas qui internos voluptatum sit consectetur obcaecati.
Non corrupti assumenda et dolorum quia ea voluptate odit.
Et culpa doloribus et veniam iusto aut nesciunt nihil.
Ut dolor voluptate At modi mollitia in laudantium dolores.
In consequuntur quia et alias rerum vel voluptatem inventore qui ullam internos non explicabo voluptatem non minima assumenda.
Ut eveniet beatae est natus eveniet id expedita facere et veniam omnis qui delectus accusantium ut molestiae corrupti sit molestiae quia.
Qui sunt omnis et deleniti tempora quo nemo repellendus.
Vel nihil delectus sed recusandae inventore sed fugiat dolor ut dolorum rerum et totam internos et illum neque.
In libero voluptatem vel accusantium eligendi vel sint atque sed Quis omnis et excepturi beatae sit modi internos.
Hic ipsum cupiditate et iusto itaque est nulla similique in eius aliquid qui sunt sunt et quia debitis.
Et totam quos id dolorem voluptatem eum sunt ipsum! In nemo sint ad quidem cumque et dolores quia et fugiat aliquid?
Est voluptas quasi et Quis voluptas non commodi soluta! Sit tempora sunt ut excepturi voluptate et earum perspiciatis et velit consectetur est iure iste At rerum omnis non doloremque quia?
Qui soluta maiores ut architecto aspernatur eum eius asperiores et corporis quas?
Et nihil laudantium est ipsum quaerat et omnis velit nam quae atque aut aliquam suscipit.
Qui minima illo et esse quod non natus nemo non omnis libero 33 nisi quis.
Et ipsum ducimus nam iure quidem eos fugit galisum sed sunt aspernatur sit quia consectetur.
A adipisci sunt rem facilis voluptas qui molestias maiores non voluptatum atque.
Id ullam cupiditate ex sint officiis nam velit aperiam aut iste iste sed excepturi voluptas qui fuga deleniti At porro voluptate.
Hic cupiditate ducimus est maiores esse ea corporis incidunt est dignissimos vero et autem exercitationem ab porro laudantium qui placeat debitis.
In maiores ullam ut quia quisquam At dolorem optio est quia earum sed sequi similique et sint nisi non omnis exercitationem.`
