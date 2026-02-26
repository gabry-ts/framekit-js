import type { Snippet, Dataset } from '../types/playground';

export const snippets: Snippet[] = [
  // Basics
  {
    category: 'Basics',
    label: 'Create DataFrame',
    code: `const result = DataFrame.fromColumns({
  name: ['Alice', 'Bob', 'Charlie', 'Diana'],
  age: [28, 35, 22, 41],
  score: [88.5, 92.0, 78.3, 95.1],
});
result.print();
return result;`,
  },
  {
    category: 'Basics',
    label: 'From rows',
    code: `const result = DataFrame.fromRows([
  { city: 'Milan', pop: 1352000, country: 'Italy' },
  { city: 'Rome',  pop: 2860000, country: 'Italy' },
  { city: 'Paris', pop: 2102000, country: 'France' },
]);
return result;`,
  },
  {
    category: 'Basics',
    label: 'Select & Drop',
    code: `const source = DataFrame.fromColumns({
  a: [1, 2, 3],
  b: [4, 5, 6],
  c: [7, 8, 9],
  d: [10, 11, 12],
});
// Select only a and b
const selected = source.select('a', 'b');
// Drop column d
const dropped = source.drop('d');
console.log('Selected:', selected.columns);
console.log('Dropped:', dropped.columns);
return selected;`,
  },

  // Filter
  {
    category: 'Filter',
    label: 'Filter with Expr',
    code: `const df = DataFrame.fromColumns({
  name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
  age: [28, 35, 22, 41, 30],
  dept: ['Eng', 'Sales', 'Eng', 'HR', 'Eng'],
});
// Filter: age > 25 AND dept == 'Eng'
const result = df.filter(
  col('age').gt(25).and(col('dept').eq('Eng'))
);
return result;`,
  },
  {
    category: 'Filter',
    label: 'Where shorthand',
    code: `const df = DataFrame.fromColumns({
  product: ['A', 'B', 'C', 'D'],
  price: [10, 25, 8, 50],
  stock: [100, 0, 45, 30],
});
// Simple where clause
const inStock = df.where('stock', '>', 0);
const affordable = inStock.where('price', '<=', 30);
return affordable;`,
  },

  // GroupBy
  {
    category: 'GroupBy',
    label: 'Group and aggregate',
    code: `const sales = DataFrame.fromColumns({
  region: ['North', 'South', 'North', 'East', 'South', 'East'],
  product: ['A', 'B', 'A', 'C', 'A', 'B'],
  revenue: [1200, 850, 2100, 950, 1600, 1100],
  quantity: [10, 8, 18, 9, 14, 10],
});
const byRegion = sales
  .groupBy('region')
  .agg({
    total_revenue: col('revenue').sum(),
    avg_quantity: col('quantity').mean(),
    orders: col('revenue').count(),
  });
return byRegion.sortBy('total_revenue', 'desc');`,
  },
  {
    category: 'GroupBy',
    label: 'Group by multiple keys',
    code: `const data = DataFrame.fromColumns({
  year: [2023, 2023, 2023, 2024, 2024, 2024],
  quarter: ['Q1', 'Q2', 'Q3', 'Q1', 'Q2', 'Q3'],
  revenue: [100, 120, 115, 130, 145, 160],
});
const grouped = data
  .groupBy('year', 'quarter')
  .agg({ total: col('revenue').sum() });
return grouped;`,
  },

  // Sort & Unique
  {
    category: 'Sort & Unique',
    label: 'Sort by multiple cols',
    code: `const df = DataFrame.fromColumns({
  dept: ['Eng', 'Sales', 'Eng', 'HR', 'Sales', 'Eng'],
  name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank'],
  salary: [90000, 75000, 85000, 80000, 70000, 95000],
});
// Sort by dept asc, then salary desc
const sorted = df.sortBy(['dept', 'salary'], ['asc', 'desc']);
return sorted;`,
  },
  {
    category: 'Sort & Unique',
    label: 'Deduplicate rows',
    code: `const df = DataFrame.fromColumns({
  city: ['Milan', 'Rome', 'Milan', 'Naples', 'Rome'],
  country: ['IT', 'IT', 'IT', 'IT', 'IT'],
  visits: [5, 3, 2, 1, 4],
});
// Unique by city (keep first occurrence)
const unique = df.unique('city', 'first');
return unique;`,
  },

  // WithColumn
  {
    category: 'Transform',
    label: 'Add computed column',
    code: `const df = DataFrame.fromColumns({
  product: ['A', 'B', 'C'],
  price: [100, 200, 150],
  quantity: [10, 5, 8],
});
// Add revenue = price * quantity
const result = df
  .withColumn('revenue', col('price').mul(col('quantity')))
  .withColumn('discount_price', col('price').mul(0.9));
return result;`,
  },
  {
    category: 'Transform',
    label: 'When / Then (case)',
    code: `const df = DataFrame.fromColumns({
  score: [95, 82, 68, 55, 45, 72, 88],
});
// Classify score into grade
const result = df.withColumn(
  'grade',
  when(col('score').gte(90))
    .then(lit('A'))
    .when(col('score').gte(80))
    .then(lit('B'))
    .when(col('score').gte(70))
    .then(lit('C'))
    .otherwise(lit('F'))
);
return result;`,
  },
  {
    category: 'Transform',
    label: 'Rename columns',
    code: `const df = DataFrame.fromColumns({
  nm: ['Alice', 'Bob'],
  ag: [28, 35],
  sc: [88, 92],
});
const renamed = df.rename({ nm: 'name', ag: 'age', sc: 'score' });
return renamed;`,
  },

  // Null handling
  {
    category: 'Null Handling',
    label: 'Fill nulls',
    code: `const df = DataFrame.fromColumns({
  name: ['Alice', null, 'Charlie', null],
  score: [88, null, 78, null],
});
// Fill nulls with specific values per column
const filled = df.fillNull({ name: 'Unknown', score: 0 });
return filled;`,
  },
  {
    category: 'Null Handling',
    label: 'Drop null rows',
    code: `const df = DataFrame.fromColumns({
  name: ['Alice', null, 'Charlie', 'Diana'],
  age: [28, 35, null, 41],
  score: [88, 92, 78, null],
});
// Drop rows where any column is null
const clean = df.dropNull();
console.log('Before:', df.length, 'rows');
console.log('After:', clean.length, 'rows');
return clean;`,
  },

  // Window functions
  {
    category: 'Window',
    label: 'Running total',
    code: `const df = DataFrame.fromColumns({
  date: ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'],
  sales: [1200, 1500, 1100, 1800, 1600],
});
const result = df.withColumn(
  'running_total',
  col('sales').cumSum()
);
return result;`,
  },
  {
    category: 'Window',
    label: 'Rolling average',
    code: `const df = DataFrame.fromColumns({
  day: [1, 2, 3, 4, 5, 6, 7],
  value: [10, 14, 8, 20, 16, 12, 18],
});
// 3-day rolling mean
const result = df.withColumn(
  'rolling_3d',
  col('value').rollingMean(3)
);
return result;`,
  },
  {
    category: 'Window',
    label: 'Rank by value',
    code: `const df = DataFrame.fromColumns({
  athlete: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
  time_ms: [1023, 987, 1045, 998, 1010],
});
const result = df.withColumn(
  'rank',
  col('time_ms').rank()
);
return result.sortBy('rank');`,
  },

  // Join
  {
    category: 'Join',
    label: 'Inner join',
    code: `const employees = DataFrame.fromColumns({
  id: [1, 2, 3, 4],
  name: ['Alice', 'Bob', 'Charlie', 'Diana'],
  dept_id: [10, 20, 10, 30],
});
const departments = DataFrame.fromColumns({
  id: [10, 20, 30],
  dept_name: ['Engineering', 'Sales', 'HR'],
});
const result = employees.join(departments, { left: 'dept_id', right: 'id' }, 'inner');
return result;`,
  },
  {
    category: 'Join',
    label: 'Left join',
    code: `const orders = DataFrame.fromColumns({
  order_id: [1, 2, 3, 4, 5],
  customer_id: [101, 102, 101, 103, 999],
  amount: [250, 180, 320, 95, 410],
});
const customers = DataFrame.fromColumns({
  id: [101, 102, 103],
  name: ['Alice', 'Bob', 'Charlie'],
});
// Left join keeps all orders even if customer not found
const result = orders.join(customers, { left: 'customer_id', right: 'id' }, 'left');
return result;`,
  },

  // Lazy
  {
    category: 'Lazy',
    label: 'Lazy execution',
    code: `const df = DataFrame.fromColumns({
  name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
  age: [28, 35, 22, 41, 30],
  score: [88, 92, 78, 95, 85],
});
// Build a lazy query (not executed yet)
const query = df.lazy()
  .filter(col('age').gt(25))
  .select('name', 'age', 'score')
  .sort('score', true);

console.log('Plan:', query.explain());

// Execute the plan
const result = await query.collect();
return result;`,
  },
  {
    category: 'Lazy',
    label: 'Lazy + groupBy',
    code: `const df = DataFrame.fromColumns({
  region: ['North', 'South', 'North', 'East', 'South'],
  value: [100, 200, 150, 300, 250],
});
const result = await df.lazy()
  .filter(col('value').gt(100))
  .groupBy('region')
  .agg(col('value').sum().as('total'))
  .collect();
return result;`,
  },

  // Pivot / Melt
  {
    category: 'Reshape',
    label: 'Pivot table',
    code: `const df = DataFrame.fromColumns({
  region: ['North', 'North', 'South', 'South'],
  product: ['A', 'B', 'A', 'B'],
  sales: [100, 200, 150, 300],
});
const pivoted = df.pivot({
  index: 'region',
  columns: 'product',
  values: 'sales',
  aggFunc: 'sum',
});
return pivoted;`,
  },
  {
    category: 'Reshape',
    label: 'Melt (unpivot)',
    code: `const df = DataFrame.fromColumns({
  name: ['Alice', 'Bob'],
  q1: [100, 120],
  q2: [110, 130],
  q3: [95, 140],
});
const melted = df.melt({
  idVars: 'name',
  valueVars: ['q1', 'q2', 'q3'],
  varName: 'quarter',
  valueName: 'sales',
});
return melted;`,
  },
];

export const datasets: Dataset[] = [
  {
    label: 'Titanic',
    filename: 'titanic.csv',
    description: '50 passengers — great for filter/groupBy',
    defaultCode: `// Titanic dataset — filter survivors by class
// __DATASETS_BASE__ is injected by the playground worker
declare const __DATASETS_BASE__: string;
const text = await fetch(__DATASETS_BASE__ + 'titanic.csv').then(r => r.text());
const df = await DataFrame.fromCSV(text, { parse: 'string', parseNumbers: true });

const result = df
  .filter(col('Survived').eq(1))
  .groupBy('Pclass')
  .agg({
    survivors: col('PassengerId').count(),
    avg_age: col('Age').mean(),
    avg_fare: col('Fare').mean(),
  })
  .sortBy('Pclass');

return result;`,
  },
  {
    label: 'Iris',
    filename: 'iris.csv',
    description: '150 flowers — great for statistics/pivot',
    defaultCode: `// Iris dataset — statistics per species
declare const __DATASETS_BASE__: string;
const text = await fetch(__DATASETS_BASE__ + 'iris.csv').then(r => r.text());
const df = await DataFrame.fromCSV(text, { parse: 'string', parseNumbers: true });

const stats = df
  .groupBy('species')
  .agg({
    count: col('sepal_length').count(),
    avg_sepal: col('sepal_length').mean(),
    avg_petal: col('petal_length').mean(),
    max_sepal: col('sepal_length').max(),
  });

return stats;`,
  },
  {
    label: 'Sales',
    filename: 'sales.csv',
    description: '50 transactions with dates — great for window functions',
    defaultCode: `// Sales dataset — revenue by region + running total
declare const __DATASETS_BASE__: string;
const text = await fetch(__DATASETS_BASE__ + 'sales.csv').then(r => r.text());
const df = await DataFrame.fromCSV(text, { parse: 'string', parseNumbers: true });

// Monthly revenue with running total
const monthly = df
  .groupBy('region')
  .agg({
    total_revenue: col('revenue').sum(),
    total_cost: col('cost').sum(),
    orders: col('revenue').count(),
  })
  .sortBy('total_revenue', 'desc');

return monthly;`,
  },
];
