#include "internal/serialize.h"

#include <malloc.h>
#include <string.h>

#include "internal/endianness.h"
#include "internal/utility.h"

int pack_container_public_info(unsigned char *buffer, uint32_t magic,
                               uint32_t version, int maximum_layer_count,
                               int container_block_size, int encryption_method,
                               int message_digest) {
  unsigned char *offset = buffer;
  pack32(buffer, offset, magic);
  pack32(buffer, offset, version);
  pack16(buffer, offset, maximum_layer_count);
  pack16(buffer, offset, container_block_size);
  pack32(buffer, offset, encryption_method);
  pack32(buffer, offset, message_digest);

  return offset - buffer;
}

int unpack_container_public_info(const unsigned char *buffer, uint32_t *magic,
                                 uint32_t *version, int *maximum_layer_count,
                                 int *container_block_size,
                                 int *encryption_method, int *message_digest) {
  const unsigned char *offset = buffer;
  unpack32(buffer, offset, *magic);
  unpack32(buffer, offset, *version);
  unpack16(buffer, offset, *maximum_layer_count);
  unpack16(buffer, offset, *container_block_size);
  unpack32(buffer, offset, *encryption_method);
  unpack32(buffer, offset, *message_digest);

  return 0;
}

int pack_layer_information_record(unsigned char *buffer,
                                  const container_t *container,
                                  const struct layer_information_t *layer) {
  unsigned char *offset = buffer;
  uint32_t crc;

  pack_bytes(buffer, offset, layer->name, sizeof(layer->name));
  pack32(buffer, offset, layer->lbi.index_blocks[0]);
  pack32(buffer, offset, layer->filesystem);
  pack32(buffer, offset, layer->filesystem_block_size);
  pack_bytes(buffer, offset, layer->lbi.key, container->cipher_key_size);
  pack_bytes(buffer, offset, layer->lbi.iv_material, container->cipher_iv_size);
  if (buffer) {
    crc = crc32c(0, buffer, offset - buffer);
  } else {
    crc = 0;
  }
  pack32(buffer, offset, crc);

  return offset - buffer;
}

int unpack_layer_information_record(const unsigned char *buffer,
                                    struct layer_information_t *layer) {
  const unsigned char *offset = buffer;
  uint32_t crc, real_crc;

  unpack_bytes(buffer, offset, layer->name, sizeof(layer->name));

  unpack32(buffer, offset, layer->lbi.index_blocks[0]);
  unpack32(buffer, offset, layer->filesystem);
  unpack32(buffer, offset, layer->filesystem_block_size);
  unpack_bytes_allocate(buffer, offset, layer->lbi.key,
                        layer->container->cipher_key_size);
  unpack_bytes_allocate(buffer, offset, layer->lbi.iv_material,
                        layer->container->cipher_iv_size);
  unpack32(buffer, offset, crc);

  if (buffer) {
    real_crc = crc32c(0, buffer, offset - buffer - sizeof(uint32_t));
    if (real_crc == crc) {
      return 0;
    }
    return -1;
  }
  return -1;
}

int pack_layer_block_index_header(unsigned char *buffer,
                                  container_block_id_t next_lbi_block_id) {
  unsigned char *offset = buffer;
  uint32_t crc;

  pack64(buffer, offset, next_lbi_block_id);

  if (buffer) {
    crc = crc32c(0, buffer, offset - buffer);
  } else {
    crc = 0;
  }
  pack32(buffer, offset, crc);

  return offset - buffer;
}

int unpack_layer_block_index_header(const unsigned char *buffer,
                                    container_block_id_t *next_lbi_block_id) {
  const unsigned char *offset = buffer;
  uint32_t crc, real_crc;

  unpack64(buffer, offset, *next_lbi_block_id);
  unpack32(buffer, offset, crc);
  if (buffer) {
    real_crc = crc32c(0, buffer, offset - buffer - sizeof(uint32_t));
    if (real_crc == crc) {
      return 0;
    }
    return -1;
  }
  return -1;
}

int pack_layer_block_index_record(unsigned char *buffer,
                                  const struct layer_information_t *layer,
                                  container_block_id_t block_id,
                                  const unsigned char *key,
                                  const unsigned char *iv) {
  unsigned char *offset = buffer;
  uint32_t crc;

  pack64(buffer, offset, block_id);
  pack_bytes(buffer, offset, key, layer->container->cipher_key_size);
  pack_bytes(buffer, offset, iv, layer->container->cipher_iv_size);
  if (buffer) {
    crc = crc32c(0, buffer, offset - buffer);
  } else {
    crc = 0;
  }
  pack32(buffer, offset, crc);

  return offset - buffer;
}

int unpack_layer_block_index_record(const unsigned char *buffer,
                                    const struct layer_information_t *layer,
                                    container_block_id_t *block_id,
                                    unsigned char *key, unsigned char *iv) {
  const unsigned char *offset = buffer;
  uint32_t crc, real_crc;

  unpack64(buffer, offset, *block_id);
  unpack_bytes(buffer, offset, key, layer->container->cipher_key_size);
  unpack_bytes(buffer, offset, iv, layer->container->cipher_iv_size);
  unpack32(buffer, offset, crc);
  if (buffer) {
    real_crc = crc32c(0, buffer, offset - buffer - sizeof(uint32_t));
    if (real_crc == crc) {
      return 0;
    }
    return -1;
  }
  return -1;
}
