/** @type {import('drizzle-kit').Config} */
export default {
  schema: './server/db/schema.js',
  out: './drizzle',
  dialect: 'sqlite',
  dbCredentials: {
    url: './data/db.sqlite'
  }
};
