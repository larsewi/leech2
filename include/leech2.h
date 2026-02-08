#ifndef __LEECH2_H__
#define __LEECH2_H__

#define SQUASH = 1

extern int lch_init(const char *work_dir);
extern int lch_block_create(void);
extern int lch_patch_create(const char *block);

#endif /* __LEECH2_H__ */
