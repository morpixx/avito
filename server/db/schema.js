import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

export const users = sqliteTable('users', {
  id: text('id').primaryKey(),
  username: text('username'),
  createdAt: integer('createdAt').notNull()
});

export const watermarks = sqliteTable('watermarks', {
  userId: text('userId').primaryKey().references(() => users.id),
  filePath: text('filePath').notNull(),
  sha256: text('sha256').notNull(),
  placement: text('placement').notNull(),
  opacity: integer('opacity').notNull(),
  margin: integer('margin').notNull(),
  updatedAt: integer('updatedAt').notNull()
});
