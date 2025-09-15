import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Hybrid Cache',
    Svg: require('@site/static/img/features/hybrid-cache.svg').default,
    description: (
      <>
        Seamlessly integrates both in-memory and disk cache for optimal performance and flexibility.
      </>
    ),
  },
  {
    title: 'Plug-and-Play Algorithms',
    Svg: require('@site/static/img/features/plug-and-play.svg').default,
    description: (
      <>
        Empowers users with easily replaceable caching algorithms, ensuring adaptability to diverse use cases.
      </>
    ),
  },
  {
    title: 'Fearless Concurrency',
    Svg: require('@site/static/img/features/fearless-concurrency.svg').default,
    description: (
      <>
        Built to handle high concurrency with robust thread-safe mechanisms, guaranteeing reliable performance under heavy loads.
      </>
    ),
  },
  {
    title: 'Zero-Copy Abstraction',
    Svg: require('@site/static/img/features/zero-copy.svg').default,
    description: (
      <>
        Leveraging Rust's robust type system, the in-memory cache in foyer achieves a better performance with zero-copy abstraction.
      </>
    ),
  },
  {
    title: 'User-Friendly Interface',
    Svg: require('@site/static/img/features/user-friendly.svg').default,
    description: (
      <>
        Offers a simple and intuitive API, making cache integration effortless and accessible for developers of all levels.
      </>
    ),
  },
  {
    title: 'Out-of-the-Box Observability',
    Svg: require('@site/static/img/features/out-of-the-box-observability.svg').default,
    description: (
      <>
        Integrate popular observation systems such as Prometheus, Grafana, Opentelemetry, and Jaeger in just <b>ONE</b> line.
      </>
    ),
  },
];

function Feature({title, Svg, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
